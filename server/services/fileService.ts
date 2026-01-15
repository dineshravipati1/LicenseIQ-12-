import fs from 'fs/promises';
import path from 'path';
import { randomUUID } from 'crypto';

export interface FileUploadResult {
  fileName: string;
  originalName: string;
  fileSize: number;
  fileType: string;
  filePath: string;
}

export interface FileValidationResult {
  isValid: boolean;
  error?: string;
}

export class FileService {
  private uploadDir: string;
  private maxFileSize: number;
  private allowedTypes: string[];

  constructor() {
    this.uploadDir = path.join(process.cwd(), 'uploads');
    this.maxFileSize = 1024 * 1024 * 1024; // 1GB
    this.allowedTypes = [
      'application/pdf',
      'application/msword',
      'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
      'text/plain'
    ];
    
    this.ensureUploadDirectory();
  }

  private async ensureUploadDirectory(): Promise<void> {
    try {
      await fs.access(this.uploadDir);
    } catch {
      await fs.mkdir(this.uploadDir, { recursive: true });
    }
  }

  validateFile(file: { size: number; mimetype: string; originalname: string }): FileValidationResult {
    // Check file size
    if (file.size > this.maxFileSize) {
      return {
        isValid: false,
        error: `File size exceeds maximum limit of ${this.maxFileSize / (1024 * 1024 * 1024)}GB`
      };
    }

    // Check file type
    if (!this.allowedTypes.includes(file.mimetype)) {
      return {
        isValid: false,
        error: `File type ${file.mimetype} is not supported. Allowed types: PDF, DOC, DOCX, TXT`
      };
    }

    // Check filename
    if (!file.originalname || file.originalname.length > 255) {
      return {
        isValid: false,
        error: 'Invalid filename'
      };
    }

    return { isValid: true };
  }

  async saveFile(fileBuffer: Buffer, originalName: string, mimeType: string): Promise<FileUploadResult> {
    const fileExtension = path.extname(originalName);
    const fileName = `${randomUUID()}${fileExtension}`;
    const filePath = path.join(this.uploadDir, fileName);
    
    await fs.writeFile(filePath, fileBuffer);

    return {
      fileName,
      originalName,
      fileSize: fileBuffer.length,
      fileType: mimeType,
      filePath: filePath,
    };
  }

  async readFile(filePath: string): Promise<Buffer> {
    return await fs.readFile(filePath);
  }

  async deleteFile(filePath: string): Promise<void> {
    try {
      await fs.unlink(filePath);
    } catch (error) {
      console.error('Error deleting file:', error);
    }
  }

  async extractTextFromFile(filePath: string, mimeType: string): Promise<string> {
    console.log('🔍 [FILE-EXTRACT] Starting text extraction...');
    console.log('🔍 [FILE-EXTRACT] File path:', filePath);
    console.log('🔍 [FILE-EXTRACT] MIME type:', mimeType);
    console.log('🔍 [FILE-EXTRACT] Node version:', process.version);
    console.log('🔍 [FILE-EXTRACT] Platform:', process.platform);
    console.log('🔍 [FILE-EXTRACT] Environment:', process.env.NODE_ENV || 'development');
    
    try {
      // Check if file exists
      try {
        await fs.access(filePath);
        console.log('✅ [FILE-EXTRACT] File exists and is accessible');
      } catch (accessError) {
        console.error('❌ [FILE-EXTRACT] File access error:', accessError);
        throw new Error(`File not accessible: ${filePath}`);
      }

      // Get file stats
      const stats = await fs.stat(filePath);
      console.log('📊 [FILE-EXTRACT] File size:', stats.size, 'bytes');
      console.log('📊 [FILE-EXTRACT] File modified:', stats.mtime);
      
      if (mimeType === 'text/plain') {
        console.log('📄 [FILE-EXTRACT] Processing text file...');
        const buffer = await fs.readFile(filePath);
        const text = buffer.toString('utf-8');
        console.log('✅ [FILE-EXTRACT] Text file processed, length:', text.length);
        return text;
      }
      
      if (mimeType === 'application/pdf') {
        console.log('📕 [FILE-EXTRACT] Processing PDF file...');
        
        try {
          // Read the PDF file
          console.log('📕 [FILE-EXTRACT] Reading PDF buffer...');
          const buffer = await fs.readFile(filePath);
          console.log('📕 [FILE-EXTRACT] PDF buffer size:', buffer.length, 'bytes');
          
          // Verify it's actually a PDF
          const pdfSignature = buffer.slice(0, 4).toString();
          console.log('📕 [FILE-EXTRACT] PDF signature:', pdfSignature);
          if (!pdfSignature.includes('%PDF')) {
            throw new Error('File does not appear to be a valid PDF (missing PDF signature)');
          }
          
          // Try multiple ways to import pdf-parse
          console.log('📕 [FILE-EXTRACT] Loading pdf-parse library...');
          let pdfParse;
          let importMethod = 'unknown';
          
          try {
            // Method 1: Dynamic import with default
            console.log('📕 [FILE-EXTRACT] Trying dynamic import with default...');
            const pdfModule = await import('pdf-parse');
            pdfParse = pdfModule.default || pdfModule;
            importMethod = 'dynamic-import-default';
            console.log('✅ [FILE-EXTRACT] pdf-parse loaded via dynamic import (default)');
          } catch (importError1) {
            const error1 = importError1 as Error;
            console.log('⚠️ [FILE-EXTRACT] Dynamic import failed:', error1.message);
            
            try {
              // Method 2: Direct dynamic import
              console.log('📕 [FILE-EXTRACT] Trying direct dynamic import...');
              pdfParse = await import('pdf-parse');
              importMethod = 'dynamic-import-direct';
              console.log('✅ [FILE-EXTRACT] pdf-parse loaded via direct dynamic import');
            } catch (importError2) {
              const error2 = importError2 as Error;
              console.log('⚠️ [FILE-EXTRACT] Direct dynamic import failed:', error2.message);
              
              try {
                // Method 3: CommonJS require
                console.log('📕 [FILE-EXTRACT] Trying CommonJS require...');
                pdfParse = require('pdf-parse');
                importMethod = 'commonjs-require';
                console.log('✅ [FILE-EXTRACT] pdf-parse loaded via CommonJS require');
              } catch (importError3) {
                const error3 = importError3 as Error;
                console.log('⚠️ [FILE-EXTRACT] CommonJS require failed:', error3.message);
                
                try {
                  // Method 4: createRequire fallback
                  console.log('📕 [FILE-EXTRACT] Trying createRequire fallback...');
                  const Module = await import('module');
                  const require = (Module as any).createRequire(import.meta.url);
                  pdfParse = require('pdf-parse');
                  importMethod = 'create-require';
                  console.log('✅ [FILE-EXTRACT] pdf-parse loaded via createRequire');
                } catch (importError4) {
                  const error4 = importError4 as Error;
                  console.error('❌ [FILE-EXTRACT] All import methods failed:');
                  console.error('  - Dynamic import default:', error1.message);
                  console.error('  - Dynamic import direct:', error2.message);
                  console.error('  - CommonJS require:', error3.message);
                  console.error('  - createRequire:', error4.message);
                  throw new Error('pdf-parse library not found. Please install: npm install pdf-parse');
                }
              }
            }
          }
          
          console.log('📕 [FILE-EXTRACT] Import method used:', importMethod);
          console.log('📕 [FILE-EXTRACT] pdf-parse type:', typeof pdfParse);
          
          // Parse the PDF
          console.log('📕 [FILE-EXTRACT] Parsing PDF content...');
          const startTime = Date.now();
          const pdfData = await pdfParse(buffer);
          const parseTime = Date.now() - startTime;
          
          console.log('📕 [FILE-EXTRACT] PDF parsing completed in', parseTime, 'ms');
          console.log('📕 [FILE-EXTRACT] PDF data structure:', {
            hasText: !!pdfData.text,
            textType: typeof pdfData.text,
            textLength: pdfData.text?.length || 0,
            hasInfo: !!pdfData.info,
            hasMetadata: !!pdfData.metadata,
            numPages: pdfData.numpages
          });
          
          // Validate extraction result
          if (!pdfData || typeof pdfData.text !== 'string') {
            console.error('❌ [FILE-EXTRACT] Invalid PDF parsing result:', {
              pdfData: !!pdfData,
              textType: typeof pdfData?.text,
              result: pdfData
            });
            throw new Error('Invalid PDF parsing result - no text extracted');
          }
          
          const extractedText = pdfData.text.trim();
          console.log('📕 [FILE-EXTRACT] Raw extracted text length:', pdfData.text.length);
          console.log('📕 [FILE-EXTRACT] Trimmed text length:', extractedText.length);
          
          // Ensure we extracted actual content, not just metadata
          if (extractedText.length < 10) {
            console.error('❌ [FILE-EXTRACT] Extracted text too short:', extractedText);
            throw new Error('PDF appears to be empty or contains no extractable text');
          }
          
          // Log sample content for debugging
          console.log('📕 [FILE-EXTRACT] ═══ EXTRACTED TEXT SAMPLE ═══');
          console.log('📕 [FILE-EXTRACT] First 300 chars:', extractedText.substring(0, 300));
          console.log('📕 [FILE-EXTRACT] Last 200 chars:', extractedText.substring(Math.max(0, extractedText.length - 200)));
          console.log('📕 [FILE-EXTRACT] ═══ END SAMPLE ═══');
          
          console.log('✅ [FILE-EXTRACT] PDF text extraction successful!');
          return extractedText;
          
        } catch (pdfError) {
          const error = pdfError as Error;
          console.error('❌ [FILE-EXTRACT] PDF parsing error:', error);
          console.error('❌ [FILE-EXTRACT] Error stack:', error.stack);
          throw new Error(`Failed to extract text from PDF: ${error.message}`);
        }
      }
      
      if (mimeType.includes('word')) {
        console.log('📝 [FILE-EXTRACT] Word document detected (not implemented)');
        return 'Word document text extraction would be implemented here with a library like mammoth';
      }
      
      console.log('⚠️ [FILE-EXTRACT] Unsupported file type:', mimeType);
      return 'Text extraction not supported for this file type';
      
    } catch (error) {
      const err = error as Error;
      console.error('❌ [FILE-EXTRACT] General extraction error:', err);
      console.error('❌ [FILE-EXTRACT] Error stack:', err.stack);
      throw new Error(`Failed to extract text from file: ${err.message}`);
    }
  }

  getFileUrl(fileName: string): string {
    return `/api/files/${fileName}`;
  }
}

export const fileService = new FileService();
