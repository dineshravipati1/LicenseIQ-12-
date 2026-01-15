import "dotenv/config";
import express, { type Request, Response, NextFunction } from "express";
import { registerRoutes } from "./routes";
import { setupVite, serveStatic, log } from "./vite";

const app = express();
app.use(express.json());
app.use(express.urlencoded({ extended: false }));

app.use((req, res, next) => {
  const start = Date.now();
  const path = req.path;
  let capturedJsonResponse: Record<string, any> | undefined = undefined;

  const originalResJson = res.json;
  res.json = function (bodyJson, ...args) {
    capturedJsonResponse = bodyJson;
    return originalResJson.apply(res, [bodyJson, ...args]);
  };

  res.on("finish", () => {
    const duration = Date.now() - start;
    if (path.startsWith("/api")) {
      let logLine = `${req.method} ${path} ${res.statusCode} in ${duration}ms`;
      if (capturedJsonResponse) {
        logLine += ` :: ${JSON.stringify(capturedJsonResponse)}`;
      }
      if (logLine.length > 80) logLine = logLine.slice(0, 79) + "…";
      log(logLine);
    }
  });

  next();
});

// Database initialization: ensure vector extension and indexes exist
async function initializeDatabase() {
  try {
    const { db } = await import("./db");
    const { sql } = await import("drizzle-orm");

    // Enable pgvector extension
    await db.execute(sql`CREATE EXTENSION IF NOT EXISTS vector;`);
    log("✓ pgvector extension enabled");

    // Create HNSW index for fast vector similarity search
    await db.execute(sql`
      CREATE INDEX IF NOT EXISTS contract_embeddings_embedding_hnsw_idx
      ON contract_embeddings
      USING hnsw (embedding vector_cosine_ops);
    `);
    log("✓ HNSW index created for vector similarity search");

    // Seed LicenseIQ Schema Catalog with standard entities
    const { seedLicenseIQSchema } = await import("./seed-licenseiq-schema");
    await seedLicenseIQSchema();

    // Seed System Knowledge Base for LIQ AI platform questions
    const { SystemKnowledgeSeeder } = await import(
      "./services/systemKnowledgeSeeder"
    );
    await SystemKnowledgeSeeder.seedKnowledgeBase();

    // Seed Navigation System (categories, items, mappings, permissions)
    const { seedNavigation } = await import("./seed-navigation");
    await seedNavigation();

    // Seed Master Data (admin user, Monrovia company hierarchy)
    const { seedMasterData } = await import("./seed-master-data");
    await seedMasterData();
  } catch (error: any) {
    log(`⚠ Database initialization warning: ${error?.message ?? String(error)}`);
    // Don't fail server startup if index creation fails
  }
}

(async () => {
  await initializeDatabase();

  // registerRoutes returns an http.Server
  const server = await registerRoutes(app);

  // Error handler (DON'T throw after responding; it crashes the server)
  app.use((err: any, _req: Request, res: Response, _next: NextFunction) => {
    const status = err?.status || err?.statusCode || 500;
    const message = err?.message || "Internal Server Error";
    log(`❌ API error: ${status} ${message}`);
    res.status(status).json({ message });
  });

  // Vite in dev; static in prod
  if (app.get("env") === "development") {
    await setupVite(app, server);
  } else {
    serveStatic(app);
  }

  /**
   * ✅ Railway / Docker fix:
   * Must listen on 0.0.0.0 so Railway can route traffic to the container.
   * Listening on 127.0.0.1 (localhost) makes it "online" but unreachable publicly.
   */
  const port = Number(process.env.PORT) || 5000;

  // In production, bind to 0.0.0.0 for Railway
  const host =
    process.env.NODE_ENV === "production"
      ? "0.0.0.0"
      : process.env.HOST || "127.0.0.1";

  server.listen(port, host, () => {
    log(`serving on http://${host}:${port}`);
  });

  server.on("error", (e: any) => {
    log(`❌ Server listen error: ${e?.message ?? String(e)}`);
  });
})();
