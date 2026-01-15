import { useState, useRef, useEffect } from "react";
import { useQuery, useMutation } from "@tanstack/react-query";
import MainLayout from "@/components/layout/main-layout";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import { Skeleton } from "@/components/ui/skeleton";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { FormattedAnswer } from "@/components/ui/formatted-answer";
import { useToast } from "@/hooks/use-toast";
import { apiRequest } from "@/lib/queryClient";
import {
  Sparkles,
  Send,
  Brain,
  FileText,
  CheckCircle2,
  AlertCircle,
  ChevronDown,
  MessageSquare,
} from "lucide-react";

interface Message {
  type: 'question' | 'answer';
  content: string;
  sources?: Array<{
    contractName: string;
    relevantText: string;
    similarity: number;
  }>;
  confidence?: number;
  timestamp: Date;
}

export default function ContractQnA() {
  const { toast } = useToast();
  const [question, setQuestion] = useState("");
  const [selectedContract, setSelectedContract] = useState<string>("all");
  const [messages, setMessages] = useState<Message[]>([]);
  const [isAsking, setIsAsking] = useState(false);
  const messagesEndRef = useRef<HTMLDivElement>(null);

  const scrollToBottom = () => {
    messagesEndRef.current?.scrollIntoView({ behavior: "smooth" });
  };

  useEffect(() => {
    scrollToBottom();
  }, [messages]);

  // Fetch all contracts
  const { data: contractsResponse, isLoading: contractsLoading } = useQuery({
    queryKey: ["/api/contracts"],
  });

  const contracts = (contractsResponse as any)?.contracts || [];

  // Ask question mutation
  const askMutation = useMutation({
    mutationFn: async () => {
      const response = await apiRequest('POST', '/api/rag/ask', {
        question,
        contractId: selectedContract === "all" ? undefined : selectedContract,
      });
      return response.json();
    },
    onSuccess: (data: any) => {
      setMessages(prev => [
        ...prev,
        { type: 'question', content: question, timestamp: new Date() },
        {
          type: 'answer',
          content: data.answer,
          sources: data.sources,
          confidence: data.confidence,
          timestamp: new Date(),
        },
      ]);
      setQuestion("");
      setIsAsking(false);
    },
    onError: (error: any) => {
      toast({
        title: "Error",
        description: error.message || "Failed to get answer",
        variant: "destructive",
      });
      setIsAsking(false);
    },
  });

  const handleAsk = () => {
    if (!question.trim()) return;
    setIsAsking(true);
    askMutation.mutate();
  };

  const getConfidenceColor = (confidence: number) => {
    if (confidence >= 0.7) return "text-green-600";
    if (confidence >= 0.5) return "text-yellow-600";
    return "text-red-600";
  };

  const getConfidenceBadge = (confidence: number) => {
    if (confidence >= 0.7) return "default";
    if (confidence >= 0.5) return "secondary";
    return "destructive";
  };

  return (
    <MainLayout title="liQ AI">
      <div className="max-w-7xl mx-auto space-y-6">
        {/* Header */}
        <div className="flex items-center justify-between">
          <div>
            <h1 className="text-3xl font-bold flex items-center gap-3">
              <div className="p-2 bg-gradient-to-br from-purple-500 to-pink-500 rounded-lg">
                <Sparkles className="h-6 w-6 text-white" />
              </div>
              liQ AI - Contract Intelligence
            </h1>
            <p className="text-muted-foreground mt-1">
              Ask questions about your contracts using AI-powered semantic search
            </p>
          </div>
        </div>

        {/* Main Chat Interface */}
        <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
          {/* Chat Area */}
          <div className="lg:col-span-2 space-y-4">
            <Card className="border-purple-200 dark:border-purple-900">
              <CardHeader className="bg-gradient-to-r from-purple-50 to-pink-50 dark:from-purple-950 dark:to-pink-950">
                <CardTitle className="flex items-center gap-2">
                  <MessageSquare className="h-5 w-5" />
                  Conversation
                </CardTitle>
                <CardDescription>
                  AI will search across all contract embeddings to find relevant information
                </CardDescription>
              </CardHeader>
              <CardContent className="p-6">
                {/* Messages */}
                <div className="space-y-4 mb-6 min-h-[400px] max-h-[500px] overflow-y-auto">
                  {messages.length === 0 ? (
                    <div className="flex flex-col items-center justify-center h-full text-center py-12">
                      <Brain className="h-16 w-16 text-purple-300 dark:text-purple-700 mb-4" />
                      <h3 className="text-lg font-semibold mb-2">Ask me anything!</h3>
                      <p className="text-sm text-muted-foreground max-w-md">
                        I can help you understand contract terms, find royalty rates, check territories, and more.
                      </p>
                    </div>
                  ) : (
                    <>
                      {messages.map((msg, idx) => (
                        <div key={idx} className={`flex ${msg.type === 'question' ? 'justify-end' : 'justify-start'}`}>
                          <div className={`max-w-[80%] ${msg.type === 'question' ? 'bg-purple-100 dark:bg-purple-900' : 'bg-gray-100 dark:bg-gray-800'} rounded-lg p-4`}>
                            {msg.type === 'question' ? (
                              <div>
                                <p className="text-sm font-semibold text-purple-700 dark:text-purple-300 mb-1">You asked:</p>
                                <p className="text-sm">{msg.content}</p>
                              </div>
                            ) : (
                              <div className="space-y-4">
                                <div className="flex items-start gap-3">
                                  <div className="p-2 bg-gradient-to-br from-purple-500 to-pink-500 rounded-lg flex-shrink-0">
                                    <Sparkles className="h-4 w-4 text-white" />
                                  </div>
                                  <div className="flex-1 space-y-3">
                                    <div className="flex items-center gap-2 mb-2">
                                      <p className="text-sm font-bold text-purple-700 dark:text-purple-300">AI Answer</p>
                                      {msg.confidence !== undefined && (
                                        <Badge variant={getConfidenceBadge(msg.confidence)} className="text-xs">
                                          <CheckCircle2 className="h-3 w-3 mr-1" />
                                          {(msg.confidence * 100).toFixed(0)}% Confidence
                                        </Badge>
                                      )}
                                    </div>
                                    <FormattedAnswer content={msg.content} />
                                    {msg.sources && msg.sources.length > 0 && (
                                      <div className="pt-3 border-t border-purple-200 dark:border-purple-800">
                                        <details className="group">
                                          <summary className="cursor-pointer text-sm font-medium text-purple-600 dark:text-purple-400 hover:text-purple-700 dark:hover:text-purple-300 flex items-center gap-2 transition-colors">
                                            <ChevronDown className="h-4 w-4 transition-transform group-open:rotate-180" />
                                            <span>View {msg.sources.length} Source{msg.sources.length > 1 ? 's' : ''}</span>
                                          </summary>
                                          <div className="mt-3 space-y-2 pl-6">
                                            {msg.sources.map((source, sidx) => (
                                              <div key={sidx} className="bg-white dark:bg-gray-900 rounded-lg p-3 border border-purple-100 dark:border-purple-900 shadow-sm hover:shadow-md transition-shadow">
                                                <div className="flex items-start justify-between gap-2 mb-2">
                                                  <div className="flex items-center gap-2 flex-1">
                                                    <FileText className="h-3.5 w-3.5 text-purple-600 dark:text-purple-400 flex-shrink-0" />
                                                    <p className="text-xs font-semibold text-purple-700 dark:text-purple-300 line-clamp-1">
                                                      {source.contractName}
                                                    </p>
                                                  </div>
                                                  <Badge variant="outline" className="text-xs flex-shrink-0">
                                                    {(source.similarity * 100).toFixed(0)}% match
                                                  </Badge>
                                                </div>
                                                <p className="text-xs text-gray-600 dark:text-gray-400 leading-relaxed pl-5">
                                                  {source.relevantText}
                                                </p>
                                              </div>
                                            ))}
                                          </div>
                                        </details>
                                      </div>
                                    )}
                                  </div>
                                </div>
                              </div>
                            )}
                          </div>
                        </div>
                      ))}
                      <div ref={messagesEndRef} />
                    </>
                  )}
                </div>

                {/* Input Area */}
                <div className="space-y-3 border-t pt-4">
                  <div className="flex gap-2">
                    <Input
                      placeholder="Ask a question about your contracts..."
                      value={question}
                      onChange={(e) => setQuestion(e.target.value)}
                      onKeyPress={(e) => e.key === 'Enter' && handleAsk()}
                      disabled={isAsking}
                      className="flex-1"
                      data-testid="input-question"
                    />
                    <Button
                      onClick={handleAsk}
                      disabled={isAsking || !question.trim()}
                      className="bg-gradient-to-r from-purple-500 to-pink-500 hover:from-purple-600 hover:to-pink-600"
                      data-testid="button-ask"
                    >
                      {isAsking ? (
                        <>Processing...</>
                      ) : (
                        <>
                          <Send className="h-4 w-4 mr-2" />
                          Ask
                        </>
                      )}
                    </Button>
                  </div>
                </div>
              </CardContent>
            </Card>
          </div>

          {/* Sidebar - Contract Selection & Info */}
          <div className="space-y-4">
            <Card>
              <CardHeader>
                <CardTitle className="text-sm">Search Scope</CardTitle>
              </CardHeader>
              <CardContent>
                <Select value={selectedContract} onValueChange={setSelectedContract}>
                  <SelectTrigger data-testid="select-contract">
                    <SelectValue placeholder="Select contract" />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="all">All Contracts</SelectItem>
                    {contracts.map((contract: any) => (
                      <SelectItem key={contract.id} value={contract.id}>
                        {contract.originalName}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
                <p className="text-xs text-muted-foreground mt-2">
                  {selectedContract === "all" 
                    ? `Searching across ${contracts.length} contracts`
                    : "Searching in selected contract"}
                </p>
              </CardContent>
            </Card>

            <Card>
              <CardHeader>
                <CardTitle className="text-sm flex items-center gap-2">
                  <FileText className="h-4 w-4" />
                  Example Questions
                </CardTitle>
              </CardHeader>
              <CardContent>
                <div className="max-h-[400px] overflow-y-auto space-y-2 pr-2">
                  <div className="text-xs font-semibold text-muted-foreground mb-2">Platform Questions:</div>
                  <div className="space-y-2">
                    {[
                      "What is LicenseIQ?",
                      "What types of contracts are supported?",
                      "How does the license fee calculation work?",
                      "What AI services are used?",
                      "How does the rule engine work?",
                      "What is the multi-tenant architecture?",
                      "What security features are available?",
                      "How does master data mapping work?"
                    ].map((example, idx) => (
                      <button
                        key={`platform-${idx}`}
                        onClick={() => setQuestion(example)}
                        className="w-full text-left text-xs p-2 rounded hover:bg-accent transition-colors border border-blue-200 bg-blue-50 dark:bg-blue-950 dark:border-blue-800"
                        data-testid={`example-question-platform-${idx}`}
                      >
                        {example}
                      </button>
                    ))}
                  </div>
                  <div className="text-xs font-semibold text-muted-foreground mb-2 mt-4">Contract Questions:</div>
                  <div className="space-y-2">
                    {[
                      "What are the license fee rates?",
                      "Which territories are covered?",
                      "What are the payment terms?",
                      "Are there any volume discounts?",
                      "What products are included?"
                    ].map((example, idx) => (
                      <button
                        key={`contract-${idx}`}
                        onClick={() => setQuestion(example)}
                        className="w-full text-left text-xs p-2 rounded hover:bg-accent transition-colors border"
                        data-testid={`example-question-contract-${idx}`}
                      >
                        {example}
                      </button>
                    ))}
                  </div>
                </div>
              </CardContent>
            </Card>

            <Card className="border-blue-200 bg-blue-50 dark:bg-blue-950 dark:border-blue-800">
              <CardContent className="pt-6">
                <div className="flex items-start gap-2">
                  <AlertCircle className="h-5 w-5 text-blue-600 dark:text-blue-400 mt-0.5" />
                  <div>
                    <h4 className="font-semibold text-blue-900 dark:text-blue-100 text-sm mb-1">
                      How it works
                    </h4>
                    <p className="text-xs text-blue-800 dark:text-blue-200">
                      liQ AI can answer questions about both the LicenseIQ platform itself and your uploaded contracts. It uses semantic search to find relevant information, then generates intelligent answers with source citations. Higher confidence scores indicate more certain answers.
                    </p>
                  </div>
                </div>
              </CardContent>
            </Card>
          </div>
        </div>
      </div>
    </MainLayout>
  );
}
