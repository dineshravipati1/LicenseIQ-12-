import { useQuery } from "@tanstack/react-query";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Database, FileText, Sparkles, TrendingUp, Layers, Hash } from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { Skeleton } from "@/components/ui/skeleton";
import { ScrollArea } from "@/components/ui/scroll-area";
import MainLayout from "@/components/layout/main-layout";
import { formatDateTimeUSA } from "@/lib/dateFormat";

interface EmbeddingStats {
  totalEmbeddings: number;
  totalChunks: number;
  totalContracts: number;
  avgChunkSize: number;
  embeddingsByType: {
    type: string;
    count: number;
  }[];
  recentEmbeddings: {
    id: string;
    contractId: string;
    contractName: string;
    embeddingType: string;
    sourceText: string;
    createdAt: string;
  }[];
}

export default function RAGDashboard() {
  const { data: stats, isLoading } = useQuery<EmbeddingStats>({
    queryKey: ['/api/rag/stats'],
  });

  if (isLoading) {
    return (
      <MainLayout title="RAG Intelligence Dashboard" description="Vector embeddings and semantic search analytics">
        <div className="space-y-6">
          <div>
            <Skeleton className="h-10 w-64 mb-2" />
            <Skeleton className="h-5 w-96" />
          </div>
          <div className="grid gap-6 md:grid-cols-2 lg:grid-cols-4">
            {[...Array(4)].map((_, i) => (
              <Skeleton key={i} className="h-32" />
            ))}
          </div>
        </div>
      </MainLayout>
    );
  }

  if (!stats) {
    return (
      <MainLayout title="RAG Intelligence Dashboard" description="Vector embeddings and semantic search analytics">
        <div>
          <p>No data available</p>
        </div>
      </MainLayout>
    );
  }

  return (
    <MainLayout title="RAG Intelligence Dashboard" description="Vector embeddings and semantic search analytics powered by Hugging Face">
      <div className="space-y-6">
      {/* Header */}
      <div>
        <h1 className="text-3xl font-bold flex items-center gap-3">
          <Sparkles className="h-8 w-8 text-purple-600" />
          RAG Intelligence Dashboard
        </h1>
        <p className="text-muted-foreground mt-1">
          Vector embeddings and semantic search analytics powered by Hugging Face
        </p>
      </div>

      {/* Metrics Grid */}
      <div className="grid gap-6 md:grid-cols-2 lg:grid-cols-4">
        {/* Total Embeddings */}
        <Card className="bg-gradient-to-br from-purple-50 to-purple-100 dark:from-purple-950 dark:to-purple-900 border-purple-200 dark:border-purple-800">
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm font-medium">Total Embeddings</CardTitle>
            <Database className="h-5 w-5 text-purple-600 dark:text-purple-400" />
          </CardHeader>
          <CardContent>
            <div className="text-3xl font-bold text-purple-700 dark:text-purple-300">
              {stats.totalEmbeddings.toLocaleString()}
            </div>
            <p className="text-xs text-purple-600 dark:text-purple-400 mt-1">
              384-dimensional vectors
            </p>
          </CardContent>
        </Card>

        {/* Total Chunks */}
        <Card className="bg-gradient-to-br from-blue-50 to-blue-100 dark:from-blue-950 dark:to-blue-900 border-blue-200 dark:border-blue-800">
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm font-medium">Text Chunks</CardTitle>
            <Layers className="h-5 w-5 text-blue-600 dark:text-blue-400" />
          </CardHeader>
          <CardContent>
            <div className="text-3xl font-bold text-blue-700 dark:text-blue-300">
              {stats.totalChunks.toLocaleString()}
            </div>
            <p className="text-xs text-blue-600 dark:text-blue-400 mt-1">
              Searchable segments
            </p>
          </CardContent>
        </Card>

        {/* Contracts Indexed */}
        <Card className="bg-gradient-to-br from-green-50 to-green-100 dark:from-green-950 dark:to-green-900 border-green-200 dark:border-green-800">
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm font-medium">Contracts Indexed</CardTitle>
            <FileText className="h-5 w-5 text-green-600 dark:text-green-400" />
          </CardHeader>
          <CardContent>
            <div className="text-3xl font-bold text-green-700 dark:text-green-300">
              {stats.totalContracts.toLocaleString()}
            </div>
            <p className="text-xs text-green-600 dark:text-green-400 mt-1">
              Ready for Q&A
            </p>
          </CardContent>
        </Card>

        {/* Avg Chunk Size */}
        <Card className="bg-gradient-to-br from-orange-50 to-orange-100 dark:from-orange-950 dark:to-orange-900 border-orange-200 dark:border-orange-800">
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm font-medium">Avg Chunk Size</CardTitle>
            <TrendingUp className="h-5 w-5 text-orange-600 dark:text-orange-400" />
          </CardHeader>
          <CardContent>
            <div className="text-3xl font-bold text-orange-700 dark:text-orange-300">
              {Math.round(stats.avgChunkSize)}
            </div>
            <p className="text-xs text-orange-600 dark:text-orange-400 mt-1">
              Characters per chunk
            </p>
          </CardContent>
        </Card>
      </div>

      {/* Embeddings by Type */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Hash className="h-5 w-5" />
            Embeddings by Type
          </CardTitle>
        </CardHeader>
        <CardContent>
          <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
            {stats.embeddingsByType.map((item) => (
              <div
                key={item.type}
                className="flex items-center justify-between p-4 rounded-lg border bg-muted/50"
              >
                <div>
                  <p className="font-medium capitalize">{item.type}</p>
                  <p className="text-sm text-muted-foreground">Embedding type</p>
                </div>
                <Badge variant="secondary" className="text-lg px-3 py-1">
                  {item.count}
                </Badge>
              </div>
            ))}
          </div>
        </CardContent>
      </Card>

      {/* Recent Embeddings */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Layers className="h-5 w-5" />
            Recent Embeddings & Chunks
          </CardTitle>
        </CardHeader>
        <CardContent>
          <ScrollArea className="h-[400px] pr-4">
            <div className="space-y-4">
              {stats.recentEmbeddings.map((embedding) => (
                <div
                  key={embedding.id}
                  className="p-4 rounded-lg border bg-card hover:bg-muted/50 transition-colors"
                  data-testid={`embedding-${embedding.id}`}
                >
                  <div className="flex items-start justify-between mb-2">
                    <div className="flex-1">
                      <div className="flex items-center gap-2 mb-1">
                        <h4 className="font-semibold text-sm">{embedding.contractName}</h4>
                        <Badge variant="outline" className="text-xs">
                          {embedding.embeddingType}
                        </Badge>
                      </div>
                      <p className="text-xs text-muted-foreground">
                        {formatDateTimeUSA(embedding.createdAt)}
                      </p>
                    </div>
                  </div>
                  <div className="mt-3 p-3 bg-muted rounded-md">
                    <p className="text-sm font-mono text-muted-foreground line-clamp-3">
                      {embedding.sourceText}
                    </p>
                    <div className="flex items-center gap-2 mt-2">
                      <Badge variant="secondary" className="text-xs">
                        {embedding.sourceText.length} chars
                      </Badge>
                      <Badge variant="secondary" className="text-xs">
                        384-dim vector
                      </Badge>
                    </div>
                  </div>
                </div>
              ))}
            </div>
          </ScrollArea>
        </CardContent>
      </Card>

      {/* Technical Info */}
      <Card className="bg-gradient-to-br from-purple-50 to-pink-50 dark:from-purple-950/50 dark:to-pink-950/50 border-purple-200 dark:border-purple-800">
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Sparkles className="h-5 w-5 text-purple-600" />
            Technical Stack
          </CardTitle>
        </CardHeader>
        <CardContent className="space-y-3">
          <div className="grid gap-4 md:grid-cols-2">
            <div>
              <h4 className="font-semibold text-sm mb-2">Embedding Model</h4>
              <p className="text-sm text-muted-foreground">
                BAAI/bge-small-en-v1.5 (Hugging Face)
              </p>
              <p className="text-xs text-muted-foreground mt-1">
                384-dimensional sentence embeddings optimized for semantic search
              </p>
            </div>
            <div>
              <h4 className="font-semibold text-sm mb-2">Vector Database</h4>
              <p className="text-sm text-muted-foreground">
                PostgreSQL with pgvector extension
              </p>
              <p className="text-xs text-muted-foreground mt-1">
                HNSW indexing for fast cosine similarity search
              </p>
            </div>
            <div>
              <h4 className="font-semibold text-sm mb-2">LLM Provider</h4>
              <p className="text-sm text-muted-foreground">
                Groq (LLaMA 3.1 8B Instant)
              </p>
              <p className="text-xs text-muted-foreground mt-1">
                Ultra-fast inference for answer generation from context
              </p>
            </div>
            <div>
              <h4 className="font-semibold text-sm mb-2">Cost</h4>
              <p className="text-sm font-bold text-green-600 dark:text-green-400">
                100% FREE
              </p>
              <p className="text-xs text-muted-foreground mt-1">
                Zero API costs using free Hugging Face & Groq tiers
              </p>
            </div>
          </div>
        </CardContent>
      </Card>
    </div>
    </MainLayout>
  );
}
