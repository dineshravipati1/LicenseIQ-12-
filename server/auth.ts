import passport from "passport";
import { Strategy as LocalStrategy } from "passport-local";
import { Express } from "express";
import session from "express-session";
import { scrypt, randomBytes, timingSafeEqual } from "crypto";
import { promisify } from "util";
import { storage } from "./storage";
import { User } from "@shared/schema";
import connectPg from "connect-pg-simple";

declare global {
  namespace Express {
    interface User extends User {}
  }
}

const scryptAsync = promisify(scrypt);

export async function hashPassword(password: string) {
  const salt = randomBytes(16).toString("hex");
  const buf = (await scryptAsync(password, salt, 64)) as Buffer;
  return `${buf.toString("hex")}.${salt}`;
}

async function comparePasswords(supplied: string, stored: string) {
  try {
    // Validate the stored password format
    if (!stored || !stored.includes('.')) {
      return false;
    }
    
    const [hashed, salt] = stored.split(".");
    if (!hashed || !salt) {
      return false;
    }
    
    const hashedBuf = Buffer.from(hashed, "hex");
    const suppliedBuf = (await scryptAsync(supplied, salt, 64)) as Buffer;
    
    // Ensure both buffers have the same length before comparison
    if (hashedBuf.length !== suppliedBuf.length) {
      return false;
    }
    
    return timingSafeEqual(hashedBuf, suppliedBuf);
  } catch (error) {
    console.error('Password comparison error:', error);
    return false;
  }
}

export function setupAuth(app: Express) {
  // Session store
  const PostgresSessionStore = connectPg(session);
  const sessionStore = new PostgresSessionStore({
    conString: process.env.DATABASE_URL,
    createTableIfMissing: true,
    ttl: 7 * 24 * 60 * 60, // 7 days
  });

  const sessionSettings: session.SessionOptions = {
    secret: process.env.SESSION_SECRET || 'your-secret-key-here',
    resave: false,
    saveUninitialized: false,
    store: sessionStore,
    cookie: {
      httpOnly: true,
      secure: false, // Set to true in production with HTTPS
      maxAge: 7 * 24 * 60 * 60 * 1000, // 7 days
    },
  };

  app.set("trust proxy", 1);
  app.use(session(sessionSettings));
  app.use(passport.initialize());
  app.use(passport.session());

  passport.use(
    new LocalStrategy(async (usernameOrEmail, password, done) => {
      try {
        // Try to find user by username first, then by email
        let user = await storage.getUserByUsername(usernameOrEmail);
        if (!user) {
          user = await storage.getUserByEmail(usernameOrEmail);
        }
        
        if (!user || !user.isActive) {
          return done(null, false, { message: 'Invalid username/email or password' });
        }
        
        const isValid = await comparePasswords(password, user.password);
        if (!isValid) {
          return done(null, false, { message: 'Invalid username/email or password' });
        }
        
        return done(null, user);
      } catch (error) {
        return done(error);
      }
    }),
  );

  passport.serializeUser((user, done) => done(null, user.id));
  passport.deserializeUser(async (id: string, done) => {
    try {
      const user = await storage.getUser(id);
      if (!user) {
        return done(null, false);
      }

      // Load user's active organization context with full details
      let activeContext = null;
      
      try {
        const userActiveCtx = await storage.getUserActiveContext(user.id);
        if (userActiveCtx) {
          // Get full context details including company/BU/location names
          const allContexts = await storage.getUserOrganizationRoles(user.id);
          const contextDetails = allContexts.find(c => c.id === userActiveCtx.activeOrgRoleId);
          
          if (contextDetails) {
            // Include all fields needed by UI: id, names, IDs, role
            activeContext = {
              id: contextDetails.id,
              userId: userActiveCtx.userId,
              activeOrgRoleId: userActiveCtx.activeOrgRoleId,
              companyId: contextDetails.companyId,
              companyName: contextDetails.companyName,
              businessUnitId: contextDetails.businessUnitId,
              businessUnitName: contextDetails.businessUnitName,
              locationId: contextDetails.locationId,
              locationName: contextDetails.locationName,
              role: contextDetails.role,
              lastSwitched: userActiveCtx.lastSwitched,
            };
          }
        }
      } catch (ctxError) {
        console.error('Error loading user context:', ctxError);
        // Continue without context - backward compatibility
      }

      // Attach active context to user object
      const userWithContext = {
        ...user,
        activeContext,
      };

      done(null, userWithContext);
    } catch (error) {
      done(error);
    }
  });

  app.post("/api/register", async (req, res, next) => {
    try {
      const { username, password, email, firstName, lastName } = req.body;
      
      // Check if username already exists
      const existingUser = await storage.getUserByUsername(username);
      if (existingUser) {
        return res.status(400).json({ message: "Username already exists" });
      }

      // Check if email already exists (if provided)
      if (email) {
        const existingEmail = await storage.getUserByEmail(email);
        if (existingEmail) {
          return res.status(400).json({ message: "Email already exists" });
        }
      }

      const hashedPassword = await hashPassword(password);
      const user = await storage.createUser({
        username,
        password: hashedPassword,
        email,
        firstName,
        lastName,
        role: "viewer", // Default role
        isActive: true,
      });

      req.login(user, (err) => {
        if (err) return next(err);
        res.status(201).json({
          id: user.id,
          username: user.username,
          email: user.email,
          firstName: user.firstName,
          lastName: user.lastName,
          role: user.role,
          isActive: user.isActive,
        });
      });
    } catch (error) {
      console.error("Registration error:", error);
      res.status(500).json({ message: "Registration failed" });
    }
  });

  app.post("/api/login", passport.authenticate("local"), async (req, res) => {
    const user = req.user as any;
    
    // Auto-initialize active context on first login if user has org assignments but no active context
    try {
      const hasActiveContext = await storage.getUserActiveContext(user.id);
      if (!hasActiveContext) {
        const userOrgRoles = await storage.getUserOrganizationRoles(user.id);
        if (userOrgRoles.length > 0) {
          // Set first org role as default active context
          await storage.setUserActiveContext(user.id, userOrgRoles[0].id);
          console.log(`✅ Auto-initialized active context for user ${user.username}`);
        }
      }
    } catch (error) {
      console.error('Error initializing context on login:', error);
      // Continue login even if context init fails
    }
    
    res.status(200).json({
      id: user.id,
      username: user.username,
      email: user.email,
      firstName: user.firstName,
      lastName: user.lastName,
      role: user.role,
      isSystemAdmin: user.isSystemAdmin || false,
      isActive: user.isActive,
      activeContext: user.activeContext || null,
    });
  });

  app.post("/api/logout", (req, res, next) => {
    req.logout((err) => {
      if (err) return next(err);
      res.sendStatus(200);
    });
  });

  app.get("/api/user", (req, res) => {
    if (!req.isAuthenticated()) {
      return res.status(401).json({ message: "Unauthorized" });
    }
    
    const user = req.user as any;
    res.json({
      id: user.id,
      username: user.username,
      email: user.email,
      firstName: user.firstName,
      lastName: user.lastName,
      role: user.role,
      isSystemAdmin: user.isSystemAdmin || false,
      isActive: user.isActive,
      activeContext: user.activeContext || null,
    });
  });
}

// Authentication middleware
export function isAuthenticated(req: any, res: any, next: any) {
  if (!req.isAuthenticated()) {
    return res.status(401).json({ message: "Unauthorized" });
  }
  
  const user = req.user as User;
  if (!user.isActive) {
    return res.status(401).json({ message: "Account is inactive" });
  }
  
  next();
}

export { comparePasswords };