import { useState, useRef, useEffect } from "react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Card } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { ScrollArea } from "@/components/ui/scroll-area";
import { FormattedAnswer } from "@/components/ui/formatted-answer";
import {
  MessageCircle,
  Send,
  X,
  Bot,
  User,
  Sparkles,
  Minimize2,
  Maximize2,
  Rocket,
  CheckCircle2,
  Loader2,
} from "lucide-react";

interface Message {
  id: string;
  role: "user" | "assistant";
  content: string;
  timestamp: Date;
  isEarlyAccessForm?: boolean;
}

export function LandingChatbot() {
  const [isOpen, setIsOpen] = useState(false);
  const [isExpanded, setIsExpanded] = useState(false);
  const [message, setMessage] = useState("");
  const [messages, setMessages] = useState<Message[]>([
    {
      id: "welcome",
      role: "assistant",
      content: "Hi! I'm liQ, your AI assistant. I can answer questions about LicenseIQ platform, help with general queries, or just chat. How can I help you today?",
      timestamp: new Date(),
    },
  ]);
  const [isLoading, setIsLoading] = useState(false);
  const [showEarlyAccessForm, setShowEarlyAccessForm] = useState(false);
  const [earlyAccessEmail, setEarlyAccessEmail] = useState("");
  const [earlyAccessName, setEarlyAccessName] = useState("");
  const [earlyAccessCompany, setEarlyAccessCompany] = useState("");
  const [isSubmittingEarlyAccess, setIsSubmittingEarlyAccess] = useState(false);
  const [earlyAccessSubmitted, setEarlyAccessSubmitted] = useState(false);
  const messagesEndRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLInputElement>(null);

  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages, showEarlyAccessForm]);

  useEffect(() => {
    if (isOpen && inputRef.current) {
      inputRef.current.focus();
    }
  }, [isOpen]);

  const generateId = () => Math.random().toString(36).substring(2, 9);

  const handleEarlyAccessSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!earlyAccessEmail || !earlyAccessEmail.includes("@")) return;

    setIsSubmittingEarlyAccess(true);

    try {
      const response = await fetch("/api/early-access-signup", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          email: earlyAccessEmail,
          name: earlyAccessName,
          company: earlyAccessCompany,
        }),
      });

      if (!response.ok) {
        throw new Error("Failed to submit");
      }

      setEarlyAccessSubmitted(true);
      setShowEarlyAccessForm(false);

      const successMessage: Message = {
        id: generateId(),
        role: "assistant",
        content: `Thank you${earlyAccessName ? `, ${earlyAccessName}` : ""}! You're now on our early access list. We'll reach out to ${earlyAccessEmail} soon with exclusive updates and your invitation to try LicenseIQ. Is there anything else you'd like to know about the platform?`,
        timestamp: new Date(),
      };
      setMessages((prev) => [...prev, successMessage]);

      setEarlyAccessEmail("");
      setEarlyAccessName("");
      setEarlyAccessCompany("");
    } catch (error) {
      console.error("Early access error:", error);
      const errorMessage: Message = {
        id: generateId(),
        role: "assistant",
        content: "I'm sorry, there was an issue submitting your request. Please try again or email us directly at info@licenseiq.ai",
        timestamp: new Date(),
      };
      setMessages((prev) => [...prev, errorMessage]);
    } finally {
      setIsSubmittingEarlyAccess(false);
    }
  };

  const handleRequestEarlyAccess = () => {
    if (earlyAccessSubmitted) {
      const alreadySubmittedMessage: Message = {
        id: generateId(),
        role: "assistant",
        content: "You're already on our early access list! We'll be in touch soon. Is there anything else you'd like to know about LicenseIQ?",
        timestamp: new Date(),
      };
      setMessages((prev) => [...prev, alreadySubmittedMessage]);
    } else {
      setShowEarlyAccessForm(true);
      const formMessage: Message = {
        id: generateId(),
        role: "assistant",
        content: "Great! I'd love to get you on our early access list. Please fill out the form below:",
        timestamp: new Date(),
      };
      setMessages((prev) => [...prev, formMessage]);
    }
  };

  const handleSend = async () => {
    if (!message.trim() || isLoading) return;

    // Hide the early access form when user sends a new message
    setShowEarlyAccessForm(false);

    const userMessage: Message = {
      id: generateId(),
      role: "user",
      content: message.trim(),
      timestamp: new Date(),
    };

    setMessages((prev) => [...prev, userMessage]);
    setMessage("");
    setIsLoading(true);

    try {
      const conversationHistory = messages
        .filter((m) => m.id !== "welcome")
        .map((m) => ({
          role: m.role,
          content: m.content,
        }));

      const response = await fetch("/api/landing-chat", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          message: userMessage.content,
          conversationHistory,
        }),
      });

      if (!response.ok) {
        throw new Error("Failed to get response");
      }

      const data = await response.json();

      const assistantMessage: Message = {
        id: generateId(),
        role: "assistant",
        content: data.response,
        timestamp: new Date(),
      };

      setMessages((prev) => [...prev, assistantMessage]);
    } catch (error) {
      console.error("Chat error:", error);
      const errorMessage: Message = {
        id: generateId(),
        role: "assistant",
        content: "I apologize, but I'm having trouble connecting right now. Please try again in a moment, or feel free to request a demo for personalized assistance!",
        timestamp: new Date(),
      };
      setMessages((prev) => [...prev, errorMessage]);
    } finally {
      setIsLoading(false);
    }
  };

  const handleKeyPress = (e: React.KeyboardEvent) => {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      handleSend();
    }
  };

  if (!isOpen) {
    return (
      <Button
        onClick={() => setIsOpen(true)}
        className="fixed bottom-6 right-6 z-50 h-14 w-14 rounded-full shadow-lg bg-gradient-to-r from-purple-600 to-blue-600 hover:from-purple-700 hover:to-blue-700 animate-pulse hover:animate-none"
        data-testid="button-open-chatbot"
      >
        <MessageCircle className="h-6 w-6 text-white" />
      </Button>
    );
  }

  return (
    <Card
      className={`fixed z-50 shadow-2xl border-0 overflow-hidden transition-all duration-300 ${
        isExpanded
          ? "bottom-4 right-4 left-4 top-4 md:left-auto md:w-[600px] md:h-[80vh]"
          : "bottom-6 right-6 w-[380px] h-[550px]"
      }`}
      data-testid="container-chatbot"
    >
      <div className="flex flex-col h-full bg-gradient-to-b from-slate-50 to-white dark:from-slate-900 dark:to-slate-800">
        <div className="flex items-center justify-between p-4 bg-gradient-to-r from-purple-600 to-blue-600 text-white">
          <div className="flex items-center gap-3">
            <div className="relative">
              <div className="w-10 h-10 rounded-full bg-white/20 flex items-center justify-center">
                <Sparkles className="h-5 w-5" />
              </div>
              <span className="absolute bottom-0 right-0 w-3 h-3 bg-green-400 rounded-full border-2 border-purple-600"></span>
            </div>
            <div>
              <h3 className="font-semibold text-lg">liQ Assistant</h3>
              <p className="text-xs text-white/80">Your AI Contract Expert</p>
            </div>
          </div>
          <div className="flex items-center gap-1">
            <Button
              variant="ghost"
              size="icon"
              onClick={() => setIsExpanded(!isExpanded)}
              className="text-white hover:bg-white/20 h-8 w-8"
              data-testid="button-toggle-expand"
            >
              {isExpanded ? (
                <Minimize2 className="h-4 w-4" />
              ) : (
                <Maximize2 className="h-4 w-4" />
              )}
            </Button>
            <Button
              variant="ghost"
              size="icon"
              onClick={() => setIsOpen(false)}
              className="text-white hover:bg-white/20 h-8 w-8"
              data-testid="button-close-chatbot"
            >
              <X className="h-4 w-4" />
            </Button>
          </div>
        </div>

        <ScrollArea className="flex-1 p-4">
          <div className="space-y-4">
            {messages.map((msg) => (
              <div
                key={msg.id}
                className={`flex gap-3 ${
                  msg.role === "user" ? "flex-row-reverse" : ""
                }`}
              >
                <div
                  className={`flex-shrink-0 w-8 h-8 rounded-full flex items-center justify-center ${
                    msg.role === "user"
                      ? "bg-purple-100 dark:bg-purple-900"
                      : "bg-gradient-to-br from-purple-500 to-blue-500"
                  }`}
                >
                  {msg.role === "user" ? (
                    <User className="h-4 w-4 text-purple-600 dark:text-purple-300" />
                  ) : (
                    <Bot className="h-4 w-4 text-white" />
                  )}
                </div>
                <div
                  className={`max-w-[80%] rounded-2xl px-4 py-2 ${
                    msg.role === "user"
                      ? "bg-purple-600 text-white rounded-tr-sm"
                      : "bg-slate-100 dark:bg-slate-700 text-slate-800 dark:text-slate-100 rounded-tl-sm"
                  }`}
                >
                  {msg.role === "assistant" ? (
                    <FormattedAnswer content={msg.content} className="text-sm" />
                  ) : (
                    <p className="text-sm whitespace-pre-wrap">{msg.content}</p>
                  )}
                </div>
              </div>
            ))}

            {showEarlyAccessForm && !earlyAccessSubmitted && (
              <div className="flex gap-3">
                <div className="flex-shrink-0 w-8 h-8 rounded-full bg-gradient-to-br from-purple-500 to-blue-500 flex items-center justify-center">
                  <Rocket className="h-4 w-4 text-white" />
                </div>
                <div className="flex-1 bg-gradient-to-br from-purple-50 to-blue-50 dark:from-purple-900/30 dark:to-blue-900/30 rounded-2xl rounded-tl-sm p-4 border border-purple-200 dark:border-purple-700">
                  <form onSubmit={handleEarlyAccessSubmit} className="space-y-3">
                    <div>
                      <Input
                        type="email"
                        placeholder="Email address *"
                        value={earlyAccessEmail}
                        onChange={(e) => setEarlyAccessEmail(e.target.value)}
                        className="bg-white dark:bg-slate-800 border-purple-200 dark:border-purple-600"
                        required
                        data-testid="input-early-access-email"
                      />
                    </div>
                    <div>
                      <Input
                        type="text"
                        placeholder="Your name (optional)"
                        value={earlyAccessName}
                        onChange={(e) => setEarlyAccessName(e.target.value)}
                        className="bg-white dark:bg-slate-800 border-purple-200 dark:border-purple-600"
                        data-testid="input-early-access-name"
                      />
                    </div>
                    <div>
                      <Input
                        type="text"
                        placeholder="Company (optional)"
                        value={earlyAccessCompany}
                        onChange={(e) => setEarlyAccessCompany(e.target.value)}
                        className="bg-white dark:bg-slate-800 border-purple-200 dark:border-purple-600"
                        data-testid="input-early-access-company"
                      />
                    </div>
                    <div className="flex gap-2">
                      <Button
                        type="submit"
                        disabled={isSubmittingEarlyAccess || !earlyAccessEmail}
                        className="flex-1 bg-gradient-to-r from-purple-600 to-blue-600 hover:from-purple-700 hover:to-blue-700"
                        data-testid="button-submit-early-access"
                      >
                        {isSubmittingEarlyAccess ? (
                          <>
                            <Loader2 className="h-4 w-4 mr-2 animate-spin" />
                            Submitting...
                          </>
                        ) : (
                          <>
                            <CheckCircle2 className="h-4 w-4 mr-2" />
                            Get Early Access
                          </>
                        )}
                      </Button>
                      <Button
                        type="button"
                        variant="outline"
                        onClick={() => setShowEarlyAccessForm(false)}
                        className="border-purple-200 dark:border-purple-600"
                        data-testid="button-cancel-early-access"
                      >
                        Cancel
                      </Button>
                    </div>
                  </form>
                </div>
              </div>
            )}

            {isLoading && (
              <div className="flex gap-3">
                <div className="flex-shrink-0 w-8 h-8 rounded-full bg-gradient-to-br from-purple-500 to-blue-500 flex items-center justify-center">
                  <Bot className="h-4 w-4 text-white" />
                </div>
                <div className="bg-slate-100 dark:bg-slate-700 rounded-2xl rounded-tl-sm px-4 py-3">
                  <div className="flex gap-1">
                    <span className="w-2 h-2 bg-purple-500 rounded-full animate-bounce" style={{ animationDelay: "0ms" }}></span>
                    <span className="w-2 h-2 bg-purple-500 rounded-full animate-bounce" style={{ animationDelay: "150ms" }}></span>
                    <span className="w-2 h-2 bg-purple-500 rounded-full animate-bounce" style={{ animationDelay: "300ms" }}></span>
                  </div>
                </div>
              </div>
            )}
            <div ref={messagesEndRef} />
          </div>
        </ScrollArea>

        <div className="p-4 border-t bg-white dark:bg-slate-800">
          <div className="flex items-center gap-2 mb-2 flex-wrap">
            <Badge 
              variant="outline" 
              className="text-xs cursor-pointer hover:bg-purple-50 dark:hover:bg-purple-900/30" 
              onClick={() => setMessage("What is LicenseIQ?")}
            >
              What is LicenseIQ?
            </Badge>
            <Badge 
              variant="outline" 
              className="text-xs cursor-pointer hover:bg-purple-50 dark:hover:bg-purple-900/30" 
              onClick={() => setMessage("Show me pricing")}
            >
              Pricing
            </Badge>
            <Badge 
              variant="outline" 
              className="text-xs cursor-pointer hover:bg-green-50 dark:hover:bg-green-900/30 border-green-300 text-green-700 dark:text-green-400" 
              onClick={handleRequestEarlyAccess}
              data-testid="badge-request-early-access"
            >
              <Rocket className="h-3 w-3 mr-1" />
              Get Early Access
            </Badge>
          </div>
          <div className="flex gap-2">
            <Input
              ref={inputRef}
              value={message}
              onChange={(e) => setMessage(e.target.value)}
              onKeyPress={handleKeyPress}
              placeholder="Ask me anything..."
              className="flex-1 rounded-full border-slate-200 dark:border-slate-600 focus-visible:ring-purple-500"
              disabled={isLoading}
              data-testid="input-chat-message"
            />
            <Button
              onClick={handleSend}
              disabled={!message.trim() || isLoading}
              className="rounded-full w-10 h-10 p-0 bg-gradient-to-r from-purple-600 to-blue-600 hover:from-purple-700 hover:to-blue-700"
              data-testid="button-send-message"
            >
              <Send className="h-4 w-4" />
            </Button>
          </div>
          <p className="text-xs text-center text-slate-400 mt-2">
            Powered By LicenseIQ
          </p>
        </div>
      </div>
    </Card>
  );
}
