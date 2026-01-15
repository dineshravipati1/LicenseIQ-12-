import { useState, useRef, useEffect } from "react";
import { useQuery, useMutation } from "@tanstack/react-query";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import { Sheet, SheetContent, SheetDescription, SheetHeader, SheetTitle, SheetTrigger } from "@/components/ui/sheet";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { FormattedAnswer } from "@/components/ui/formatted-answer";
import { useToast } from "@/hooks/use-toast";
import { apiRequest } from "@/lib/queryClient";
import {
  Sparkles,
  Send,
  Brain,
  CheckCircle2,
  ChevronDown,
  MessageSquare,
  X,
  FileText,
} from "lucide-react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";

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

export function FloatingAIAssistant() {
  const { toast } = useToast();
  const [isOpen, setIsOpen] = useState(false);
  const [question, setQuestion] = useState("");
  const [selectedContract, setSelectedContract] = useState<string>("all");
  const [messages, setMessages] = useState<Message[]>([]);
  const [isAsking, setIsAsking] = useState(false);
  const messagesEndRef = useRef<HTMLDivElement>(null);

  // Auto-scroll to bottom when new messages arrive
  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages]);

  // Fetch all contracts
  const { data: contractsResponse } = useQuery({
    queryKey: ["/api/contracts"],
    enabled: isOpen, // Only fetch when panel is open
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

  const getConfidenceBadge = (confidence: number) => {
    if (confidence >= 0.7) return "default";
    if (confidence >= 0.5) return "secondary";
    return "destructive";
  };

  const clearChat = () => {
    setMessages([]);
    setQuestion("");
  };

  return (
    <Sheet open={isOpen} onOpenChange={setIsOpen} modal={false}>
      <SheetTrigger asChild>
        <Button
          size="lg"
          className="fixed bottom-6 right-6 h-14 w-14 rounded-full shadow-2xl bg-gradient-to-br from-purple-500 to-pink-500 hover:from-purple-600 hover:to-pink-600 hover:scale-110 transition-all duration-200 z-50 group"
          data-testid="button-ai-assistant"
          aria-label="Open AI Assistant"
        >
          <Sparkles className="h-6 w-6 text-white group-hover:animate-pulse" />
        </Button>
      </SheetTrigger>
      <SheetContent side="right" className="w-full sm:w-[540px] p-0 overflow-hidden" onPointerDownOutside={(e) => e.preventDefault()}>
        <SheetHeader className="p-6 pb-4 bg-gradient-to-r from-purple-50 to-pink-50 dark:from-purple-950 dark:to-pink-950 border-b">
          <SheetTitle className="flex items-center gap-2 text-xl">
            <div className="p-2 bg-gradient-to-br from-purple-500 to-pink-500 rounded-lg">
              <Sparkles className="h-5 w-5 text-white" />
            </div>
            liQ Agent
          </SheetTitle>
          <SheetDescription>
            Ask questions about both the LicenseIQ platform and your contracts using AI-powered semantic search
          </SheetDescription>
        </SheetHeader>

        <div className="flex flex-col h-[calc(100vh-140px)]">
          {/* Search Scope */}
          <div className="p-4 border-b bg-gray-50 dark:bg-gray-900">
            <label className="text-xs font-semibold text-muted-foreground mb-2 block">
              Search Scope
            </label>
            <Select value={selectedContract} onValueChange={setSelectedContract}>
              <SelectTrigger data-testid="select-contract-ai">
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
            <p className="text-xs text-muted-foreground mt-1">
              {selectedContract === "all" 
                ? `Searching across ${contracts.length} contract${contracts.length !== 1 ? 's' : ''}`
                : "Searching in selected contract"}
            </p>
          </div>

          {/* Messages */}
          <div className="flex-1 overflow-y-auto p-4 space-y-4 bg-white dark:bg-gray-950">
            {messages.length === 0 ? (
              <div className="flex flex-col items-center justify-center h-full text-center py-12">
                <Brain className="h-16 w-16 text-purple-300 dark:text-purple-700 mb-4" />
                <h3 className="text-lg font-semibold mb-2">Ask me anything!</h3>
                <p className="text-sm text-muted-foreground max-w-md mb-4">
                  I can answer questions about the LicenseIQ platform itself and your contracts - including terms, rates, territories, and features.
                </p>
                <div className="space-y-2 w-full max-w-sm">
                  <p className="text-xs font-semibold text-muted-foreground text-left mb-1">Try asking:</p>
                  {[
                    "What is LicenseIQ?",
                    "How does the rule engine work?",
                    "What are the license fee rates?",
                    "Which territories are covered?",
                  ].map((example, idx) => (
                    <button
                      key={idx}
                      onClick={() => setQuestion(example)}
                      className="w-full text-left text-xs p-2 rounded hover:bg-purple-50 dark:hover:bg-purple-950 transition-colors border text-muted-foreground hover:text-foreground"
                      data-testid={`example-question-ai-${idx}`}
                    >
                      💬 {example}
                    </button>
                  ))}
                </div>
              </div>
            ) : (
              <>
                {messages.map((msg, idx) => (
                  <div key={idx} className={`flex ${msg.type === 'question' ? 'justify-end' : 'justify-start'}`}>
                    <div className={`max-w-[85%] ${msg.type === 'question' ? 'bg-purple-100 dark:bg-purple-900' : 'bg-gray-100 dark:bg-gray-800'} rounded-lg p-3`}>
                      {msg.type === 'question' ? (
                        <div>
                          <p className="text-xs font-semibold text-purple-700 dark:text-purple-300 mb-1">You:</p>
                          <p className="text-sm">{msg.content}</p>
                        </div>
                      ) : (
                        <div className="space-y-3">
                          <div className="flex items-start gap-2">
                            <div className="p-1.5 bg-gradient-to-br from-purple-500 to-pink-500 rounded-lg flex-shrink-0">
                              <Sparkles className="h-3 w-3 text-white" />
                            </div>
                            <div className="flex-1 space-y-2">
                              <div className="flex items-center gap-2">
                                <p className="text-xs font-bold text-purple-700 dark:text-purple-300">AI Answer</p>
                                {msg.confidence !== undefined && (
                                  <Badge variant={getConfidenceBadge(msg.confidence)} className="text-xs">
                                    <CheckCircle2 className="h-3 w-3 mr-1" />
                                    {(msg.confidence * 100).toFixed(0)}%
                                  </Badge>
                                )}
                              </div>
                              <FormattedAnswer content={msg.content} className="text-xs" />
                              {msg.sources && msg.sources.length > 0 && (
                                <div className="pt-2 border-t border-purple-200 dark:border-purple-800">
                                  <details className="group">
                                    <summary className="cursor-pointer text-xs font-medium text-purple-600 dark:text-purple-400 hover:text-purple-700 dark:hover:text-purple-300 flex items-center gap-1 transition-colors">
                                      <ChevronDown className="h-3 w-3 transition-transform group-open:rotate-180" />
                                      <span>View {msg.sources.length} Source{msg.sources.length > 1 ? 's' : ''}</span>
                                    </summary>
                                    <div className="mt-2 space-y-2 pl-4">
                                      {msg.sources.map((source, sidx) => (
                                        <div key={sidx} className="bg-white dark:bg-gray-900 rounded p-2 border border-purple-100 dark:border-purple-900 shadow-sm">
                                          <div className="flex items-start justify-between gap-2 mb-1">
                                            <div className="flex items-center gap-1.5 flex-1">
                                              <FileText className="h-3 w-3 text-purple-600 dark:text-purple-400 flex-shrink-0" />
                                              <p className="font-semibold text-purple-700 dark:text-purple-300 text-xs line-clamp-1">
                                                {source.contractName}
                                              </p>
                                            </div>
                                            <Badge variant="outline" className="text-xs flex-shrink-0">
                                              {(source.similarity * 100).toFixed(0)}%
                                            </Badge>
                                          </div>
                                          <p className="text-gray-600 dark:text-gray-400 text-xs leading-relaxed pl-4">
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
          <div className="p-4 border-t bg-gray-50 dark:bg-gray-900">
            {messages.length > 0 && (
              <div className="mb-2 flex justify-end">
                <Button
                  variant="ghost"
                  size="sm"
                  onClick={clearChat}
                  className="text-xs"
                  data-testid="button-clear-chat"
                >
                  <X className="h-3 w-3 mr-1" />
                  Clear Chat
                </Button>
              </div>
            )}
            <div className="flex gap-2">
              <Input
                placeholder="Ask a question..."
                value={question}
                onChange={(e) => setQuestion(e.target.value)}
                onKeyPress={(e) => e.key === 'Enter' && handleAsk()}
                disabled={isAsking}
                className="flex-1"
                data-testid="input-question-ai"
              />
              <Button
                onClick={handleAsk}
                disabled={isAsking || !question.trim()}
                className="bg-gradient-to-r from-purple-500 to-pink-500 hover:from-purple-600 hover:to-pink-600"
                data-testid="button-ask-ai"
              >
                {isAsking ? (
                  <>...</>
                ) : (
                  <Send className="h-4 w-4" />
                )}
              </Button>
            </div>
          </div>
        </div>
      </SheetContent>
    </Sheet>
  );
}
