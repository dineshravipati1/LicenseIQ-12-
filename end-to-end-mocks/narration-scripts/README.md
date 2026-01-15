# Voice Narration Scripts for License IQ Demo

This folder contains step-by-step narration scripts for creating professional demo videos of the License IQ platform.

## 📁 Files Overview

| Script File | Screen | Duration | Purpose |
|------------|--------|----------|---------|
| `00-master-script.txt` | All screens | ~5 min 20s | Complete end-to-end narration |
| `01-contract-upload-script.txt` | Contract Upload | ~30s | Uploading PDF contracts |
| `02-ai-analysis-script.txt` | AI Processing | ~35s | Contract analysis with AI |
| `03-analysis-results-script.txt` | Analysis Results | ~45s | Extracted terms & rules |
| `04-sales-upload-script.txt` | Sales Upload | ~30s | Uploading sales data |
| `05-ai-matching-script.txt` | AI Matching | ~40s | Semantic matching process |
| `06-royalty-dashboard-script.txt` | Royalty Dashboard | ~50s | Calculations & visualizations |
| `07-invoice-generation-script.txt` | Invoice Generation | ~35s | PDF invoice creation |
| `08-rag-qna-script.txt` | Q&A Assistant | ~45s | Contract Q&A with RAG |

## 🎬 How to Use These Scripts

### Option 1: Professional Voice Recording
1. **Text-to-Speech Services:**
   - Use [ElevenLabs](https://elevenlabs.io/) for AI voice generation
   - Use [Murf.ai](https://murf.ai/) for professional voiceovers
   - Use [Amazon Polly](https://aws.amazon.com/polly/) for scalable TTS

2. **Recording Process:**
   - Import each script file
   - Select a professional voice (recommend: male/female neutral tone)
   - Adjust pace to ~150 words per minute
   - Export as MP3/WAV files

### Option 2: Personal Voice Recording
1. **Setup:**
   - Use a good microphone (USB condenser mic recommended)
   - Record in a quiet room
   - Use Audacity (free) or Adobe Audition

2. **Recording Tips:**
   - Speak clearly at a moderate pace
   - Pause at [STEP] markers for emphasis
   - Use consistent tone throughout
   - Leave 1-2 second pauses between sections

### Option 3: Auto-Generated (Quick)
1. Use built-in browser TTS:
   ```javascript
   const utterance = new SpeechSynthesisUtterance(scriptText);
   utterance.rate = 0.9; // Slightly slower for clarity
   speechSynthesis.speak(utterance);
   ```

## 🎥 Video Production Workflow

### Step 1: Prepare Audio Files
1. Record/generate voice for each script
2. Name audio files to match: `01-contract-upload.mp3`, etc.
3. Store in same folder for easy access

### Step 2: Screen Recording
1. Open each HTML mock screen in sequence
2. Use OBS Studio or similar screen recorder
3. Match video timing to audio duration
4. Include mouse movements and highlights

### Step 3: Video Editing
1. Import screen recordings to video editor (DaVinci Resolve/Premiere)
2. Sync audio narration with corresponding screen
3. Add transitions between screens (1-2 second fade)
4. Include background music (low volume, ~20%)

### Step 4: Final Touches
- Add text overlays for key points
- Highlight UI elements when mentioned
- Add zoom effects for important details
- Include intro/outro slides with branding

## 📝 Script Structure

Each script follows this format:

```
SCREEN X: [TITLE]
Duration: ~XX seconds

[INTRO]
Opening context

[STEP 1]
First action/observation

[STEP 2]
Second action/observation

[TRANSITION]
Link to next screen
```

## 🎯 Key Features to Emphasize

When recording, emphasize these selling points:
- ✅ **100% FREE AI** - No API costs (Groq + Hugging Face)
- ✅ **Automated everything** - From upload to invoice
- ✅ **High accuracy** - AI confidence scores shown
- ✅ **Complex calculations** - Handles tiered rates, seasonal adjustments
- ✅ **Professional output** - PDF invoices ready to send

## 🔊 Voice Tone Guidelines

**Overall Tone:** Professional yet approachable, confident but not arrogant

**Pacing:**
- Intro sections: Slower, welcoming
- Technical details: Clear, methodical
- Results/benefits: Enthusiastic, proud
- Transitions: Natural, conversational

**Emphasis Points:**
- Product features: Strong, confident
- Numbers/metrics: Clear enunciation
- Benefits: Excited, positive
- Technical terms: Precise pronunciation

## 📊 Recommended Voice Settings

### ElevenLabs Settings:
- Voice: "Josh" (professional male) or "Rachel" (professional female)
- Stability: 60%
- Clarity + Similarity: 75%
- Style Exaggeration: 25%

### Murf.ai Settings:
- Voice: "Matt" (business) or "Sarah" (corporate)
- Speed: 1.0x (normal)
- Pitch: 0 (neutral)
- Emphasis: Medium

## 📹 Final Video Specs

**Recommended Export Settings:**
- Resolution: 1920x1080 (Full HD)
- Frame Rate: 30 fps
- Bitrate: 8-10 Mbps
- Audio: 192 kbps AAC
- Format: MP4 (H.264)

**Platform-Specific:**
- YouTube: 1080p, 30fps, MP4
- LinkedIn: 720p-1080p, MP4, <10 min
- Website: 720p, MP4, compressed

## 🚀 Quick Start

1. **5-Minute Complete Demo:**
   - Use `00-master-script.txt`
   - Record all HTML screens in sequence
   - One continuous narration

2. **Individual Features (30s each):**
   - Pick specific scripts for targeted demos
   - Create short feature highlights
   - Perfect for social media

3. **Full Walkthrough (6-8 minutes):**
   - Use all 8 individual scripts
   - Add intro (30s) and outro (30s)
   - Include transitions and highlights

## 📞 Support

For questions about script content or suggestions for improvements, refer to the main project documentation.

---

**Created:** October 14, 2025  
**Purpose:** Professional demo video creation  
**Platform:** License IQ Research Platform
