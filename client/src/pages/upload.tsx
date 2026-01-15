import { useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useAuth } from "@/hooks/useAuth";
import MainLayout from "@/components/layout/main-layout";
import FileUpload from "@/components/upload/file-upload";
import { Card, CardContent } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Label } from "@/components/ui/label";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Textarea } from "@/components/ui/textarea";
import { Checkbox } from "@/components/ui/checkbox";
import { useToast } from "@/hooks/use-toast";
import { useLocation } from "wouter";
import { apiRequest } from "@/lib/queryClient";

export default function Upload() {
  const { user } = useAuth();
  const { toast } = useToast();
  const queryClient = useQueryClient();
  const [, setLocation] = useLocation();

  const [selectedFile, setSelectedFile] = useState<File | null>(null);
  const [contractType, setContractType] = useState("license");
  const [priority, setPriority] = useState("normal");
  const [notes, setNotes] = useState("");
  const [processingOptions, setProcessingOptions] = useState({
    aiAnalysis: true,
    extractTerms: true,
    riskAssessment: false,
    complianceCheck: false,
  });

  const uploadMutation = useMutation({
    mutationFn: async () => {
      if (!selectedFile) {
        throw new Error("No file selected");
      }

      const formData = new FormData();
      formData.append("file", selectedFile);
      formData.append("contractType", contractType);
      formData.append("priority", priority);
      formData.append("notes", notes);

      const response = await apiRequest("POST", "/api/contracts/upload", formData);
      return response.json();
    },
    onSuccess: async (data) => {
      toast({
        title: "Upload Successful",
        description: "Your contract has been uploaded and processing has started.",
      });
      
      // Comprehensive cache invalidation for immediate UI updates
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: ["/api/contracts"] }),
        queryClient.invalidateQueries({ queryKey: ["/api/analytics/metrics"] }),
        queryClient.invalidateQueries({ queryKey: ["/api/analytics/all"] }),
        // Force immediate refetch to ensure UI shows new contract
        queryClient.refetchQueries({ queryKey: ["/api/contracts"] }),
        queryClient.refetchQueries({ queryKey: ["/api/analytics/metrics"] })
      ]);
      
      // Small delay to ensure database transaction is committed
      await new Promise(resolve => setTimeout(resolve, 100));
      
      // Redirect to contract view
      setLocation(`/contracts/${data.id}`);
    },
    onError: (error: Error) => {
      toast({
        title: "Upload Failed",
        description: error.message,
        variant: "destructive",
      });
    },
  });

  const handleSubmit = () => {
    if (!selectedFile) {
      toast({
        title: "No File Selected",
        description: "Please select a file to upload.",
        variant: "destructive",
      });
      return;
    }

    uploadMutation.mutate();
  };

  return (
    <MainLayout 
      title="Upload New Contract" 
      description="Drag and drop your contract files or click to browse"
    >
      <div className="max-w-4xl mx-auto space-y-6">
        {/* File Upload */}
        <Card>
          <CardContent className="p-8">
            <FileUpload 
              onFileSelect={setSelectedFile}
              selectedFile={selectedFile}
              isUploading={uploadMutation.isPending}
            />
          </CardContent>
        </Card>

        {/* Configuration */}
        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
          {/* Processing Options */}
          <Card>
            <CardContent className="p-6 space-y-4">
              <h4 className="font-medium text-foreground">Processing Options</h4>
              <div className="space-y-3">
                <div className="flex items-center space-x-2">
                  <Checkbox
                    id="ai-analysis"
                    checked={processingOptions.aiAnalysis}
                    onCheckedChange={(checked) =>
                      setProcessingOptions(prev => ({ ...prev, aiAnalysis: !!checked }))
                    }
                    data-testid="checkbox-ai-analysis"
                  />
                  <Label htmlFor="ai-analysis" className="text-sm">
                    AI Analysis & Summarization
                  </Label>
                </div>
                <div className="flex items-center space-x-2">
                  <Checkbox
                    id="extract-terms"
                    checked={processingOptions.extractTerms}
                    onCheckedChange={(checked) =>
                      setProcessingOptions(prev => ({ ...prev, extractTerms: !!checked }))
                    }
                    data-testid="checkbox-extract-terms"
                  />
                  <Label htmlFor="extract-terms" className="text-sm">
                    Extract Key Terms
                  </Label>
                </div>
                <div className="flex items-center space-x-2">
                  <Checkbox
                    id="risk-assessment"
                    checked={processingOptions.riskAssessment}
                    onCheckedChange={(checked) =>
                      setProcessingOptions(prev => ({ ...prev, riskAssessment: !!checked }))
                    }
                    data-testid="checkbox-risk-assessment"
                  />
                  <Label htmlFor="risk-assessment" className="text-sm">
                    Risk Assessment
                  </Label>
                </div>
                <div className="flex items-center space-x-2">
                  <Checkbox
                    id="compliance-check"
                    checked={processingOptions.complianceCheck}
                    onCheckedChange={(checked) =>
                      setProcessingOptions(prev => ({ ...prev, complianceCheck: !!checked }))
                    }
                    data-testid="checkbox-compliance-check"
                  />
                  <Label htmlFor="compliance-check" className="text-sm">
                    Compliance Check
                  </Label>
                </div>
              </div>
            </CardContent>
          </Card>

          {/* Contract Details */}
          <Card>
            <CardContent className="p-6 space-y-4">
              <h4 className="font-medium text-foreground">Contract Details</h4>
              <div className="space-y-4">
                <div>
                  <Label htmlFor="contract-type">Contract Type</Label>
                  <Select value={contractType} onValueChange={setContractType}>
                    <SelectTrigger data-testid="select-contract-type">
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="license">License Agreement</SelectItem>
                      <SelectItem value="service">Service Agreement</SelectItem>
                      <SelectItem value="partnership">Partnership Agreement</SelectItem>
                      <SelectItem value="employment">Employment Contract</SelectItem>
                      <SelectItem value="other">Other</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
                
                <div>
                  <Label htmlFor="priority">Priority</Label>
                  <Select value={priority} onValueChange={setPriority}>
                    <SelectTrigger data-testid="select-priority">
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="normal">Normal</SelectItem>
                      <SelectItem value="high">High</SelectItem>
                      <SelectItem value="urgent">Urgent</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
                
                <div>
                  <Label htmlFor="notes">Notes</Label>
                  <Textarea
                    id="notes"
                    value={notes}
                    onChange={(e) => setNotes(e.target.value)}
                    placeholder="Optional notes about this contract..."
                    rows={3}
                    data-testid="textarea-notes"
                  />
                </div>
              </div>
            </CardContent>
          </Card>
        </div>

        {/* Actions */}
        <div className="flex justify-end space-x-3">
          <Button
            variant="outline"
            onClick={() => setLocation("/contracts")}
            data-testid="button-cancel"
          >
            Cancel
          </Button>
          <Button
            onClick={handleSubmit}
            disabled={!selectedFile || uploadMutation.isPending}
            data-testid="button-start-processing"
          >
            {uploadMutation.isPending ? "Processing..." : "Start Processing"}
          </Button>
        </div>
      </div>
    </MainLayout>
  );
}
