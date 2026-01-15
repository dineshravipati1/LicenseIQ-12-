import { useQuery } from "@tanstack/react-query";
import MainLayout from "@/components/layout/main-layout";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Skeleton } from "@/components/ui/skeleton";
import { ScrollArea } from "@/components/ui/scroll-area";
import { BookOpen, Brain, Sparkles, FileText, Search } from "lucide-react";
import { Input } from "@/components/ui/input";
import { useState } from "react";
import {
  Accordion,
  AccordionContent,
  AccordionItem,
  AccordionTrigger,
} from "@/components/ui/accordion";

interface KnowledgeEntry {
  id: string;
  category: string;
  title: string;
  content: string;
  createdAt: string;
}

export default function KnowledgeBase() {
  const [searchQuery, setSearchQuery] = useState("");

  const { data: entries = [], isLoading } = useQuery<KnowledgeEntry[]>({
    queryKey: ['/api/knowledge-base'],
  });

  const filteredEntries = entries.filter(entry => 
    entry.title.toLowerCase().includes(searchQuery.toLowerCase()) ||
    entry.content.toLowerCase().includes(searchQuery.toLowerCase()) ||
    entry.category.toLowerCase().includes(searchQuery.toLowerCase())
  );

  const categories = [...new Set(filteredEntries.map(e => e.category))];

  if (isLoading) {
    return (
      <MainLayout title="Knowledge Base" description="Platform knowledge and documentation">
        <div className="space-y-6">
          <Skeleton className="h-10 w-64 mb-2" />
          <Skeleton className="h-12 w-full" />
          <div className="grid gap-4">
            {[...Array(4)].map((_, i) => (
              <Skeleton key={i} className="h-32" />
            ))}
          </div>
        </div>
      </MainLayout>
    );
  }

  return (
    <MainLayout title="Knowledge Base" description="Platform knowledge and documentation for liQ AI">
      <div className="space-y-6">
        <div className="flex items-center justify-between">
          <div>
            <h1 className="text-3xl font-bold flex items-center gap-3">
              <BookOpen className="h-8 w-8 text-blue-600" />
              Knowledge Base
            </h1>
            <p className="text-muted-foreground mt-1">
              System knowledge used by liQ AI to answer platform questions
            </p>
          </div>
          <Badge variant="outline" className="text-lg px-4 py-2">
            <Brain className="h-4 w-4 mr-2" />
            {entries.length} Entries
          </Badge>
        </div>

        <div className="relative">
          <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 h-4 w-4 text-muted-foreground" />
          <Input
            placeholder="Search knowledge base..."
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            className="pl-10"
            data-testid="input-search-knowledge"
          />
        </div>

        <div className="grid gap-6 md:grid-cols-4">
          <Card>
            <CardHeader className="pb-2">
              <CardTitle className="text-sm font-medium text-muted-foreground">Total Entries</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold">{entries.length}</div>
            </CardContent>
          </Card>
          <Card>
            <CardHeader className="pb-2">
              <CardTitle className="text-sm font-medium text-muted-foreground">Categories</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold">{categories.length}</div>
            </CardContent>
          </Card>
          <Card>
            <CardHeader className="pb-2">
              <CardTitle className="text-sm font-medium text-muted-foreground">Search Results</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold">{filteredEntries.length}</div>
            </CardContent>
          </Card>
          <Card>
            <CardHeader className="pb-2">
              <CardTitle className="text-sm font-medium text-muted-foreground">AI Integration</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="flex items-center gap-2">
                <Sparkles className="h-5 w-5 text-purple-500" />
                <span className="text-sm font-medium text-green-600">Active</span>
              </div>
            </CardContent>
          </Card>
        </div>

        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <FileText className="h-5 w-5" />
              Knowledge Entries
            </CardTitle>
          </CardHeader>
          <CardContent>
            <ScrollArea className="h-[600px] pr-4">
              {categories.length === 0 ? (
                <div className="text-center py-12 text-muted-foreground">
                  <BookOpen className="h-12 w-12 mx-auto mb-4 opacity-50" />
                  <p>No knowledge entries found</p>
                </div>
              ) : (
                <Accordion type="multiple" className="space-y-4">
                  {categories.map((category) => (
                    <AccordionItem key={category} value={category} className="border rounded-lg px-4">
                      <AccordionTrigger className="text-lg font-semibold hover:no-underline">
                        <div className="flex items-center gap-2">
                          <Badge variant="secondary">{category}</Badge>
                          <span className="text-sm text-muted-foreground">
                            ({filteredEntries.filter(e => e.category === category).length} entries)
                          </span>
                        </div>
                      </AccordionTrigger>
                      <AccordionContent>
                        <div className="space-y-4 pt-2">
                          {filteredEntries
                            .filter(e => e.category === category)
                            .map((entry) => (
                              <div key={entry.id} className="border-l-4 border-blue-500 pl-4 py-2">
                                <h4 className="font-medium text-foreground">{entry.title}</h4>
                                <p className="text-sm text-muted-foreground mt-1 whitespace-pre-line">
                                  {entry.content.length > 500 
                                    ? entry.content.substring(0, 500) + "..."
                                    : entry.content
                                  }
                                </p>
                              </div>
                            ))}
                        </div>
                      </AccordionContent>
                    </AccordionItem>
                  ))}
                </Accordion>
              )}
            </ScrollArea>
          </CardContent>
        </Card>
      </div>
    </MainLayout>
  );
}
