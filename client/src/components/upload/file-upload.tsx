import { useCallback, useState } from "react";
import { useDropzone } from "react-dropzone";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import { Progress } from "@/components/ui/progress";
import { Badge } from "@/components/ui/badge";
import { cn } from "@/lib/utils";
import { 
  Upload, 
  File, 
  X, 
  CheckCircle, 
  AlertCircle,
  FileText,
  FileSpreadsheet
} from "lucide-react";

interface FileUploadProps {
  onFileSelect: (file: File | null) => void;
  selectedFile: File | null;
  isUploading?: boolean;
  uploadProgress?: number;
  className?: string;
}

export default function FileUpload({ 
  onFileSelect, 
  selectedFile, 
  isUploading = false, 
  uploadProgress = 0,
  className 
}: FileUploadProps) {
  const [dragActive, setDragActive] = useState(false);

  const onDrop = useCallback((acceptedFiles: File[]) => {
    if (acceptedFiles.length > 0) {
      onFileSelect(acceptedFiles[0]);
    }
    setDragActive(false);
  }, [onFileSelect]);

  const onDragEnter = useCallback(() => {
    setDragActive(true);
  }, []);

  const onDragLeave = useCallback(() => {
    setDragActive(false);
  }, []);

  const { getRootProps, getInputProps, isDragActive, fileRejections } = useDropzone({
    onDrop,
    onDragEnter,
    onDragLeave,
    accept: {
      'application/pdf': ['.pdf'],
      'application/msword': ['.doc'],
      'application/vnd.openxmlformats-officedocument.wordprocessingml.document': ['.docx'],
      'text/plain': ['.txt']
    },
    maxSize: 1024 * 1024 * 1024, // 1GB
    multiple: false,
    disabled: isUploading
  });

  const getFileIcon = (file: File) => {
    if (file.type === 'application/pdf') {
      return <FileText className="h-6 w-6 text-red-400" />;
    } else if (file.type.includes('word') || file.type.includes('document')) {
      return <FileSpreadsheet className="h-6 w-6 text-blue-400" />;
    }
    return <File className="h-6 w-6 text-slate-400" />;
  };

  const formatFileSize = (bytes: number) => {
    if (bytes === 0) return '0 Bytes';
    const k = 1024;
    const sizes = ['Bytes', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
  };

  const removeFile = () => {
    onFileSelect(null);
  };

  return (
    <div className={cn("space-y-4", className)}>
      {/* Upload Zone */}
      {!selectedFile && (
        <div
          {...getRootProps()}
          className={cn(
            "upload-zone w-full h-64 flex flex-col items-center justify-center rounded-lg cursor-pointer transition-all",
            "border-2 border-dashed",
            isDragActive || dragActive 
              ? "border-primary bg-primary/5" 
              : "border-border hover:border-primary hover:bg-primary/5",
            isUploading && "opacity-50 cursor-not-allowed"
          )}
          data-testid="file-upload-zone"
        >
          <input {...getInputProps()} />
          <div className="text-center">
            <Upload className="h-12 w-12 text-violet-400 mb-4 mx-auto" />
            <h3 className="text-lg font-medium text-foreground mb-2">
              {isDragActive ? "Drop files here" : "Choose files or drag here"}
            </h3>
            <p className="text-muted-foreground mb-4">
              Supports PDF, DOC, DOCX, TXT up to 1GB
            </p>
            <Button variant="outline" type="button" disabled={isUploading}>
              Browse Files
            </Button>
          </div>
        </div>
      )}

      {/* File Rejections */}
      {fileRejections.length > 0 && (
        <div className="space-y-2">
          {fileRejections.map(({ file, errors }) => (
            <Card key={file.name} className="border-destructive">
              <CardContent className="p-4">
                <div className="flex items-center space-x-3">
                  <AlertCircle className="h-5 w-5 text-destructive" />
                  <div className="flex-1">
                    <p className="font-medium text-foreground">{file.name}</p>
                    <div className="text-sm text-destructive">
                      {errors.map(error => error.message).join(', ')}
                    </div>
                  </div>
                </div>
              </CardContent>
            </Card>
          ))}
        </div>
      )}

      {/* Selected File */}
      {selectedFile && (
        <Card data-testid="selected-file">
          <CardContent className="p-4">
            <div className="flex items-center space-x-4">
              {getFileIcon(selectedFile)}
              <div className="flex-1">
                <div className="flex items-center justify-between">
                  <p className="font-medium text-foreground">{selectedFile.name}</p>
                  <div className="flex items-center space-x-2">
                    <Badge variant="outline">
                      {formatFileSize(selectedFile.size)}
                    </Badge>
                    {!isUploading && (
                      <Button
                        variant="ghost"
                        size="sm"
                        onClick={removeFile}
                        data-testid="button-remove-file"
                      >
                        <X className="h-4 w-4" />
                      </Button>
                    )}
                  </div>
                </div>
                
                {isUploading && (
                  <div className="mt-2 space-y-2">
                    <Progress value={uploadProgress} className="w-full" />
                    <div className="flex items-center justify-between text-xs text-muted-foreground">
                      <span>Uploading... {uploadProgress}%</span>
                      {uploadProgress < 100 && (
                        <span>Processing contract...</span>
                      )}
                    </div>
                  </div>
                )}
                
                {!isUploading && (
                  <div className="flex items-center mt-1 text-sm text-muted-foreground">
                    <CheckCircle className="h-4 w-4 mr-1 text-green-500" />
                    Ready to upload
                  </div>
                )}
              </div>
            </div>
          </CardContent>
        </Card>
      )}
    </div>
  );
}
