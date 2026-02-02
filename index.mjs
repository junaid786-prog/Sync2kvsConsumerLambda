/**
 * Sync2 KVS Consumer Lambda
 * --------------------------
 * Reads raw audio from Kinesis Video Stream and routes to voice AI backend.
 * Supports dual-mode operation controlled by VOICE_AI_MODE environment variable.
 *
 * Modes:
 *   - "legacy" (default): Audio → Faster Whisper STT → SQS transcripts
 *   - "ultravox": Audio → Ultravox S2S server → SQS audio responses
 *
 * Flow:
 * 1. Receives KVS stream ARN and call metadata from sipLambda
 * 2. Connects to KVS and reads audio fragments (MKV format)
 * 3. Extracts PCM audio from MKV container
 * 4. Routes to appropriate backend based on VOICE_AI_MODE:
 *    - Legacy: Streams to Faster Whisper STT, sends transcripts to SQS
 *    - Ultravox: Streams to Ultravox S2S, sends audio responses to SQS
 *
 * Environment Variables:
 *   VOICE_AI_MODE - "legacy" or "ultravox" (default: "legacy")
 *   FASTER_WHISPER_STT_URL - WebSocket URL for Whisper STT (legacy mode)
 *   ULTRAVOX_SERVER_URL - WebSocket URL for Ultravox S2S (ultravox mode)
 */

import {
  KinesisVideoClient,
  GetDataEndpointCommand,
} from "@aws-sdk/client-kinesis-video";
import {
  KinesisVideoMediaClient,
  GetMediaCommand,
} from "@aws-sdk/client-kinesis-video-media";
import {
  SQSClient,
  SendMessageCommand,
} from "@aws-sdk/client-sqs";
import pg from "pg";
import WebSocket from "ws";

const { Pool } = pg;

const wait = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

// ---------- AI PHRASE FILTER ----------
// These patterns detect AI-generated speech that gets transcribed from mixed audio
// This prevents the AI from processing its own TTS output as customer speech
const AI_PHRASE_PATTERNS = [
  // Greetings & Welcome
  /how may i help/i, /how can i help/i, /how can i assist/i, /how may i assist/i,
  /welcome to.*medical/i, /rpb medical/i, /pharmasync/i,
  /thank you for calling/i, /thanks for calling/i,

  // AI Response patterns
  /i.?d be happy to/i, /i.?ll be happy to/i, /happy to help/i,
  /i can help you/i, /let me help/i,
  /sure,? i.?d/i, /sure thing/i, /certainly/i, /absolutely/i, /of course/i,
  /no problem/i, /no worries/i,

  // AI Actions
  /let me (see|check|look|find|get|help|assist|know|refocus)/i,
  /let me see/i, /let me check/i,
  /i.?ll (need|check|look|get|help)/i,

  // Scheduling phrases (AI typically says these)
  /what type of appointment/i, /looking to (book|schedule)/i,
  /can i get your/i, /may i have your/i, /could you (provide|give|tell)/i,
  /get that scheduled/i, /help you schedule/i,

  // Clarifications
  /not sure i caught/i, /could you repeat/i, /didn.?t quite catch/i,
  /you mentioned/i, /you said/i, /you need/i,
  // More clarification phrases
  /not quite sure what you mean/i, /hmm,? i.?m not/i, /i.?m not quite sure/i,
  /could you clarify/i, /what do you mean by/i, /can you explain/i,
  /i want to make sure/i, /make sure i (help|understand)/i,
  /are you looking for/i, /looking for a specific/i,

  // Farewells (AI says these)
  /take care/i, /have a (great|good|wonderful|nice) day/i,
  /thanks (so much )?for (calling|reaching out)/i,
  /is there anything else/i, /anything else i can/i,

  // Common AI filler/acknowledgments
  /^alright[,.]?$/i, /^okay[,.]?$/i, /^got it[,.]?$/i, /^awesome[,.]?!?$/i,
  /^perfect[,.]?$/i, /^great[,.]?$/i, /^sure[,.]?$/i,

  // Hallucinated phrases from distorted TTS
  /we love you/i, /thanks for watching/i, /thank you for watching/i, /see you (next|in the)/i,
  /subscribe/i, /like and subscribe/i,

  // Short AI phrases that get transcribed (often during barge-in or distorted)
  /^thank you\.?$/i, /^thank you[,.]? ?(bye|goodbye)?\.?$/i,
  /^bye[,.]?$/i, /^goodbye[,.]?$/i,
  /thank you\.? thank you/i,
  /^hello[,.]?$/i, /^hi[,.]?$/i,
  /^yes[,.]?$/i, /^yeah[,.]?$/i, /^and[,.]?$/i,
];

/**
 * Check if transcript matches AI speech patterns
 */
function isAiGeneratedPhrase(transcript) {
  if (!transcript) return false;
  const text = transcript.trim();
  return AI_PHRASE_PATTERNS.some(pattern => pattern.test(text));
}

// ---------- CONFIG ----------
const REGION = process.env.AWS_REGION || "us-east-1";
const FASTER_WHISPER_STT_URL = process.env.FASTER_WHISPER_STT_URL || "ws://44.216.12.223:8766";
const TRANSCRIPT_QUEUE_URL_GROUP = process.env.TRANSCRIPT_QUEUE_URL_GROUP;

// Voice AI Mode Configuration
const VOICE_AI_MODE = (process.env.VOICE_AI_MODE || "legacy").toLowerCase().trim();
const ULTRAVOX_SERVER_URL = process.env.ULTRAVOX_SERVER_URL || "ws://44.216.12.223:8770/ultravox";

/**
 * Check if running in Ultravox S2S mode
 */
function isUltravoxMode() {
  return VOICE_AI_MODE === "ultravox";
}

const PG_CONFIG = {
  host: process.env.PGHOST,
  port: parseInt(process.env.PGPORT || "5432"),
  database: process.env.PGDATABASE,
  user: process.env.PGUSER,
  password: process.env.PGPASSWORD,
  ssl: { rejectUnauthorized: false, require: true },
};

// ---------- CLIENTS ----------
const kinesisVideo = new KinesisVideoClient({ region: REGION });
const sqs = new SQSClient({ region: REGION });
const pgPool = new Pool(PG_CONFIG);

// ---------- HANDLER ----------
export const handler = async (event) => {
  console.log("🎙️ KVS Consumer Lambda invoked");
  console.log("📥 Event:", JSON.stringify(event, null, 2));

  // Handle direct invocation from sipLambda
  if (event.source === "sipLambda" && event.streamArn) {
    return processKVSStream(event);
  }

  // Handle SQS trigger (alternative approach)
  if (event.Records?.[0]?.eventSource === "aws:sqs") {
    for (const record of event.Records) {
      try {
        const msg = JSON.parse(record.body);
        if (msg.streamArn) {
          await processKVSStream(msg);
        }
      } catch (err) {
        console.error("❌ Failed to parse SQS message:", err);
      }
    }
    return { statusCode: 200, body: "OK" };
  }

  return { statusCode: 400, body: "Unknown event source" };
};

// ---------- MAIN PROCESSING ----------
async function processKVSStream(event) {
  const {
    streamArn,
    callSessionId,
    callId,
    fromE164,
    toE164,
    mipId,
  } = event;

  console.log(`📞 Processing KVS stream for call ${callId}`);
  console.log(`   Stream ARN: ${streamArn}`);
  console.log(`   From: ${fromE164} → To: ${toE164}`);

  try {
    // Step 1: Get the data endpoint for the KVS stream
    const endpointResponse = await kinesisVideo.send(
      new GetDataEndpointCommand({
        StreamARN: streamArn,
        APIName: "GET_MEDIA",
      })
    );

    const dataEndpoint = endpointResponse.DataEndpoint;
    console.log(`📍 KVS Data Endpoint: ${dataEndpoint}`);

    // Step 2: Create KVS Media client with the endpoint
    const kvsMediaClient = new KinesisVideoMediaClient({
      region: REGION,
      endpoint: dataEndpoint,
    });

    // Step 3: Get media from the stream
    const mediaResponse = await kvsMediaClient.send(
      new GetMediaCommand({
        StreamARN: streamArn,
        StartSelector: {
          StartSelectorType: "NOW",
        },
      })
    );

    console.log(`🎵 Content-Type: ${mediaResponse.ContentType}`);

    // Step 4: Get channel ID for the call (may not be attached immediately)
    let channelId = await getChannelForSession(callSessionId);
    if (!channelId) {
      console.warn(`⚠️ Channel ID missing for call_session ${callSessionId}. Waiting for ARI to attach...`);
      channelId = await waitForChannelAttachment(callSessionId);
      if (!channelId) {
        console.warn(`⚠️ Still no channel mapping for call_session ${callSessionId}. Proceeding with session fallback.`);
      }
    }

    // Step 5: Process audio stream with Faster Whisper
    await processAudioStream(
      mediaResponse.Payload,
      callSessionId,
      channelId,
      mipId,
      fromE164,
      toE164
    );

    console.log(`✅ KVS processing completed for call ${callId}`);
    return { statusCode: 200, body: "Processed" };

  } catch (err) {
    console.error(`❌ KVS processing error: ${err.message}`);
    console.error(err.stack);
    return { statusCode: 500, body: err.message };
  }
}

// ---------- AUDIO STREAM PROCESSING ----------

/**
 * Process audio stream with Ultravox S2S
 * Routes audio directly to Ultravox server which handles STT+LLM+TTS in one pipeline
 */
async function processAudioStreamUltravox(payloadStream, callSessionId, initialChannelId, mipId, fromE164, toE164) {
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(ULTRAVOX_SERVER_URL);
    let isConnected = false;
    let fragmentCount = 0;
    let channelId = initialChannelId;

    // MKV parser state
    const mkvParser = new MKVAudioExtractor();

    // Audio buffer for accumulating PCM data
    let pcmBuffer = Buffer.alloc(0);
    const CHUNK_SIZE = 3200; // 200ms at 8kHz 16-bit mono

    // Stats
    let audioBytesSent = 0;
    let audioChunksReceived = 0;

    ws.on("open", async () => {
      console.log(`🔌 [ULTRAVOX] Connected to: ${ULTRAVOX_SERVER_URL}`);
      isConnected = true;

      // Wait for channel ID if not available
      if (!channelId) {
        channelId = await waitForChannelAttachment(callSessionId, 20, 250);
      }

      // Configure the Ultravox session
      ws.send(JSON.stringify({
        type: "config",
        session_id: callSessionId,
        channel_id: channelId,
        sample_rate: 8000,  // KVS audio is 8kHz, Ultravox server will resample
        system_prompt: `You are a helpful medical office assistant for RPB Medical.
Be concise and friendly. Help callers with appointments, refills, and general inquiries.
Keep responses brief - this is a phone conversation.`,
      }));

      console.log(`🎤 [ULTRAVOX] Session configured for channel ${channelId}`);
    });

    ws.on("message", async (data) => {
      // In Node.js ws library, all messages come as Buffer by default
      // Try to parse as JSON first, fall back to binary handling
      const dataStr = data.toString();

      // Check if it looks like JSON (starts with { or [)
      if (dataStr.startsWith('{') || dataStr.startsWith('[')) {
        // JSON message from Ultravox
        try {
          const event = JSON.parse(dataStr);
          const eventType = event.type || "";

          if (eventType === "ready") {
            console.log(`✅ [ULTRAVOX] Server ready`);
          } else if (eventType === "transcript") {
            const speaker = event.speaker || "unknown";
            const text = event.text || "";
            const isFinal = event.is_final || false;

            console.log(`📝 [ULTRAVOX] ${speaker} ${isFinal ? "(final)" : "(partial)"}: "${text.substring(0, 50)}..."`);

            if (!channelId) {
              channelId = await waitForChannelAttachment(callSessionId, 5, 200);
            }

            // For agent responses, send as ultravox_response for ARI bridge to TTS
            if (speaker === "agent" && isFinal && text.length > 2) {
              console.log(`🔊 [ULTRAVOX] Sending AI response for TTS: "${text.substring(0, 50)}..."`);
              await sendUltravoxResponseToQueue({
                text,
                callSessionId,
                channelId,
                mipId,
                meta: { fromE164, toE164 },
              });
            }
          } else if (eventType === "user_speech_start") {
            console.log(`🎤 [ULTRAVOX] User started speaking`);
            // Notify ARI Bridge of barge-in
            await sendBargeInToQueue(callSessionId, channelId, 0.9);
          } else if (eventType === "speech_start") {
            console.log(`🔊 [ULTRAVOX] AI started speaking`);
          } else if (eventType === "speech_end") {
            console.log(`🔇 [ULTRAVOX] AI finished speaking`);
          } else if (eventType === "listening") {
            console.log(`👂 [ULTRAVOX] Listening for user...`);
          } else if (eventType === "error") {
            console.error(`❌ [ULTRAVOX] Error: ${event.error}`);
          } else {
            console.log(`📨 [ULTRAVOX] Unknown message type: ${eventType}`);
          }
        } catch (err) {
          console.error(`❌ [ULTRAVOX] Error parsing JSON message: ${err.message}`);
          console.error(`   Raw data (first 100 chars): ${dataStr.substring(0, 100)}`);
        }
      } else {
        // Binary data (audio from Ultravox - not used in current architecture)
        audioChunksReceived++;
        if (audioChunksReceived % 10 === 0) {
          console.log(`🔊 [ULTRAVOX] Received ${audioChunksReceived} binary chunks (ignored - using text responses)`);
        }
      }
    });

    ws.on("close", () => {
      console.log(`🔌 [ULTRAVOX] Connection closed. Fragments: ${fragmentCount}, Audio sent: ${(audioBytesSent/1024).toFixed(1)}KB`);
      resolve();
    });

    ws.on("error", (err) => {
      console.error(`❌ [ULTRAVOX] WebSocket error: ${err.message}`);
      reject(err);
    });

    ws.on("ping", () => {
      ws.pong();
    });

    // Process the KVS payload stream
    payloadStream.on("data", (chunk) => {
      if (!isConnected) return;

      fragmentCount++;

      // Extract audio from MKV container
      const audioFrames = mkvParser.addChunk(chunk);

      for (const audioData of audioFrames) {
        if (audioData && audioData.length > 0) {
          pcmBuffer = Buffer.concat([pcmBuffer, audioData]);

          // Send chunks when we have enough data
          while (pcmBuffer.length >= CHUNK_SIZE) {
            const sendSize = Math.floor(CHUNK_SIZE / 2) * 2;
            const toSend = pcmBuffer.slice(0, sendSize);
            pcmBuffer = pcmBuffer.slice(sendSize);

            ws.send(toSend);
            audioBytesSent += toSend.length;
          }
        }
      }

      if (fragmentCount % 100 === 0) {
        console.log(`📊 [ULTRAVOX] Fragments: ${fragmentCount}, Sent: ${(audioBytesSent/1024).toFixed(1)}KB`);
      }
    });

    payloadStream.on("end", () => {
      console.log(`📭 [ULTRAVOX] KVS stream ended. Total fragments: ${fragmentCount}`);

      // Send remaining buffered audio
      if (pcmBuffer.length > 0 && isConnected && ws.readyState === WebSocket.OPEN) {
        const finalSize = Math.floor(pcmBuffer.length / 2) * 2;
        if (finalSize > 0) {
          ws.send(pcmBuffer.slice(0, finalSize));
          audioBytesSent += finalSize;
        }
      }

      // Signal end
      if (isConnected && ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({ type: "end" }));
        setTimeout(() => {
          if (ws.readyState === WebSocket.OPEN) {
            ws.close();
          }
        }, 5000);
      } else {
        resolve();
      }
    });

    payloadStream.on("error", (err) => {
      console.error(`❌ [ULTRAVOX] KVS stream error: ${err.message}`);
      if (ws.readyState === WebSocket.OPEN) {
        ws.close();
      }
      reject(err);
    });

    // Timeout after 5 minutes
    setTimeout(() => {
      console.log("⏰ [ULTRAVOX] Processing timeout reached");
      if (ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({ type: "end" }));
        setTimeout(() => ws.close(), 2000);
      }
    }, 5 * 60 * 1000);
  });
}

/**
 * Process audio stream - routes to appropriate handler based on VOICE_AI_MODE
 */
async function processAudioStream(payloadStream, callSessionId, initialChannelId, mipId, fromE164, toE164) {
  console.log(`🎤 Voice AI Mode: ${VOICE_AI_MODE.toUpperCase()}`);

  if (isUltravoxMode()) {
    console.log(`🚀 Using Ultravox S2S pipeline`);
    return processAudioStreamUltravox(payloadStream, callSessionId, initialChannelId, mipId, fromE164, toE164);
  }

  console.log(`📡 Using Legacy STT pipeline (Faster Whisper)`);
  return processAudioStreamLegacy(payloadStream, callSessionId, initialChannelId, mipId, fromE164, toE164);
}

/**
 * Process audio stream with Legacy STT (Faster Whisper)
 */
async function processAudioStreamLegacy(payloadStream, callSessionId, initialChannelId, mipId, fromE164, toE164) {
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(FASTER_WHISPER_STT_URL);
    let lastTranscriptTime = 0;
    let isConnected = false;
    let fragmentCount = 0;

    // MKV parser state
    const mkvParser = new MKVAudioExtractor();

    // Audio buffer for accumulating PCM data before sending
    // Send chunks of ~3200 bytes (200ms of 8kHz 16-bit mono audio)
    let pcmBuffer = Buffer.alloc(0);
    const CHUNK_SIZE = 3200; // 200ms at 8kHz 16-bit mono

    ws.on("open", () => {
      console.log(`🔌 Connected to Faster Whisper STT: ${FASTER_WHISPER_STT_URL}`);
      isConnected = true;

      // Configure the STT session
      // KVS audio is typically 8kHz PCM from phone calls
      ws.send(JSON.stringify({
        type: "config",
        sample_rate: 8000,
        encoding: "pcm16",
        call_session_id: callSessionId,
      }));
    });

    let channelId = initialChannelId;

    ws.on("message", async (data) => {
      try {
        const event = JSON.parse(data.toString());
        const eventType = event.type || "";

        if (eventType === "transcript") {
          const transcript = (event.transcript || "").trim();
          const isPartial = event.is_partial || false;

          if (!transcript || transcript.length < 3) return;

          // 🎤 AI PHRASE FILTER: Block AI self-transcription at source
          // This prevents the mixed audio (caller + AI TTS) from causing feedback loops
          if (isAiGeneratedPhrase(transcript)) {
            console.log(`🔇 [AI-FILTER] Blocked ${isPartial ? "partial" : "FINAL"}: "${transcript.substring(0, 60)}..."`);
            return; // Don't send AI speech to SQS
          }

          // Rate limit partials
          const now = Date.now();
          if (isPartial && (now - lastTranscriptTime) < 500) return;
          lastTranscriptTime = now;

          if (!channelId) {
            channelId = await waitForChannelAttachment(callSessionId, 10, 250);
          }

          console.log(`📝 ${isPartial ? "Partial" : "Final"}: "${transcript.substring(0, 50)}..."`);

          // Send to SQS
          await sendTranscriptToQueue({
            transcript,
            mipId,
            isPartial,
            callSessionId,
            channelId,
            meta: { fromE164, toE164 },
            source: "faster-whisper",
          });

        } else if (eventType === "barge_in") {
          // 🚀 VAD-BASED BARGE-IN: Send immediately for faster interruption (~200ms vs ~800ms)
          // This bypasses transcript filtering for instant playback stop
          console.log(`🎤 Barge-in detected! Confidence: ${event.confidence?.toFixed(2)}`);

          if (!channelId) {
            channelId = await waitForChannelAttachment(callSessionId, 5, 200);
          }

          // Send barge_in signal to ARI bridge for immediate playback stop
          await sendBargeInToQueue(callSessionId, channelId, event.confidence || 0.8);

        } else if (eventType === "silence") {
          console.log(`🔇 Silence: ${event.duration_ms}ms`);
        }
      } catch (err) {
        console.error(`❌ Error parsing STT response: ${err.message}`);
      }
    });

    ws.on("close", () => {
      console.log(`🔌 Faster Whisper connection closed. Processed ${fragmentCount} fragments.`);
      resolve();
    });

    ws.on("error", (err) => {
      console.error(`❌ WebSocket error: ${err.message}`);
      reject(err);
    });

    // Handle ping/pong to keep connection alive
    // Server sends ping every 60s, we must respond with pong
    ws.on("ping", () => {
      console.log(`🏓 Received ping from STT server, sending pong`);
      ws.pong();
    });

    // Process the KVS payload stream
    // KVS returns MKV-formatted data with audio fragments
    payloadStream.on("data", (chunk) => {
      if (!isConnected) return;

      fragmentCount++;

      // Extract audio from MKV container using proper parser
      const audioFrames = mkvParser.addChunk(chunk);

      for (const audioData of audioFrames) {
        if (audioData && audioData.length > 0) {
          // Accumulate audio in buffer
          pcmBuffer = Buffer.concat([pcmBuffer, audioData]);

          // Send chunks when we have enough data
          while (pcmBuffer.length >= CHUNK_SIZE) {
            // Ensure we send even number of bytes (16-bit PCM)
            const sendSize = Math.floor(CHUNK_SIZE / 2) * 2;
            const toSend = pcmBuffer.slice(0, sendSize);
            pcmBuffer = pcmBuffer.slice(sendSize);

            ws.send(toSend);
          }
        }
      }

      if (fragmentCount % 100 === 0) {
        console.log(`📊 Processed ${fragmentCount} audio fragments, buffer: ${pcmBuffer.length} bytes`);
      }
    });

    payloadStream.on("end", () => {
      console.log(`📭 KVS stream ended. Total fragments: ${fragmentCount}`);

      // Send any remaining buffered audio (ensure even byte count)
      if (pcmBuffer.length > 0 && isConnected && ws.readyState === WebSocket.OPEN) {
        const finalSize = Math.floor(pcmBuffer.length / 2) * 2;
        if (finalSize > 0) {
          ws.send(pcmBuffer.slice(0, finalSize));
        }
      }

      // Signal end of audio
      if (isConnected && ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({ type: "end" }));

        // Give time for final transcripts
        setTimeout(() => {
          if (ws.readyState === WebSocket.OPEN) {
            ws.close();
          }
        }, 5000);
      } else {
        resolve();
      }
    });

    payloadStream.on("error", (err) => {
      console.error(`❌ KVS stream error: ${err.message}`);
      if (ws.readyState === WebSocket.OPEN) {
        ws.close();
      }
      reject(err);
    });

    // Timeout after 5 minutes (max Lambda duration consideration)
    setTimeout(() => {
      console.log("⏰ Processing timeout reached");
      if (ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({ type: "end" }));
        setTimeout(() => ws.close(), 2000);
      }
    }, 5 * 60 * 1000);
  });
}

// ---------- MKV AUDIO EXTRACTION ----------
/**
 * MKV Element IDs used in KVS streams
 */
const MKV_ELEMENTS = {
  EBML: 0x1A45DFA3,
  SEGMENT: 0x18538067,
  CLUSTER: 0x1F43B675,
  TIMECODE: 0xE7,
  SIMPLE_BLOCK: 0xA3,
  BLOCK: 0xA1,
  BLOCK_GROUP: 0xA0,
  TRACKS: 0x1654AE6B,
  TRACK_ENTRY: 0xAE,
  TRACK_NUMBER: 0xD7,
  TRACK_TYPE: 0x83,
  CODEC_ID: 0x86,
};

/**
 * MKV Audio Extractor - Properly parses MKV/WebM container format
 * to extract raw PCM audio frames from KVS streams.
 */
class MKVAudioExtractor {
  constructor() {
    this.buffer = Buffer.alloc(0);
    this.audioTrackNumber = 1; // Default, updated from track info
    this.headerParsed = false;
    this.inCluster = false;
  }

  /**
   * Add a chunk of MKV data and return extracted audio frames
   */
  addChunk(chunk) {
    this.buffer = Buffer.concat([this.buffer, chunk]);
    const audioFrames = [];

    while (this.buffer.length > 0) {
      const result = this.parseElement();
      if (!result) break;

      const { consumed, audioData } = result;

      if (audioData) {
        audioFrames.push(audioData);
      }

      this.buffer = this.buffer.slice(consumed);
    }

    return audioFrames;
  }

  /**
   * Parse a single EBML element from the buffer
   */
  parseElement() {
    if (this.buffer.length < 2) return null;

    // Read element ID (variable length, 1-4 bytes)
    const idResult = this.readVINT(this.buffer, 0, true);
    if (!idResult) return null;
    const { value: elementId, length: idLen } = idResult;

    if (this.buffer.length < idLen + 1) return null;

    // Read element size (variable length)
    const sizeResult = this.readVINT(this.buffer, idLen, false);
    if (!sizeResult) return null;
    const { value: elementSize, length: sizeLen } = sizeResult;

    const headerLen = idLen + sizeLen;

    // Handle unknown/very large sizes (streaming mode)
    const isUnknownSize = elementSize === -1;

    // For container elements, we don't need the full content
    // For data elements (SimpleBlock), we do
    if (elementId === MKV_ELEMENTS.SIMPLE_BLOCK || elementId === MKV_ELEMENTS.BLOCK) {
      // Need full element data
      if (!isUnknownSize && this.buffer.length < headerLen + elementSize) {
        return null; // Wait for more data
      }

      const audioData = this.parseSimpleBlock(
        this.buffer.slice(headerLen, headerLen + elementSize)
      );

      return {
        consumed: headerLen + elementSize,
        audioData,
      };
    }

    // Container elements - just skip the header and continue parsing children
    if (
      elementId === MKV_ELEMENTS.EBML ||
      elementId === MKV_ELEMENTS.SEGMENT ||
      elementId === MKV_ELEMENTS.CLUSTER ||
      elementId === MKV_ELEMENTS.TRACKS ||
      elementId === MKV_ELEMENTS.TRACK_ENTRY ||
      elementId === MKV_ELEMENTS.BLOCK_GROUP
    ) {
      if (elementId === MKV_ELEMENTS.CLUSTER) {
        this.inCluster = true;
      }
      // Return header consumed, children will be parsed in next iterations
      return { consumed: headerLen, audioData: null };
    }

    // Skip other elements
    if (!isUnknownSize && this.buffer.length >= headerLen + elementSize) {
      return { consumed: headerLen + elementSize, audioData: null };
    }

    // Can't process yet
    return null;
  }

  /**
   * Parse a SimpleBlock element to extract audio data
   * Note: KVS from Chime Voice Connector typically has single track (mixed audio)
   * The AI feedback loop issue needs to be solved at ARIend level
   */
  parseSimpleBlock(data) {
    if (data.length < 4) return null;

    // Track number (variable length integer)
    const trackResult = this.readVINT(data, 0, false);
    if (!trackResult) return null;

    const trackNum = trackResult.value;
    const trackLen = trackResult.length;

    // Log track info occasionally for debugging (every 1000th block)
    if (Math.random() < 0.001) {
      console.log(`🎵 Audio block from track ${trackNum}`);
    }

    // Note: Track filtering disabled - KVS from Chime has single mixed track
    // The feedback loop (AI hearing itself) needs to be handled in ARIend
    // by ignoring transcripts that arrive while AI is speaking

    // Timestamp (2 bytes, signed big-endian)
    if (data.length < trackLen + 2) return null;
    // const timestamp = data.readInt16BE(trackLen);

    // Flags (1 byte)
    if (data.length < trackLen + 3) return null;
    // const flags = data[trackLen + 2];

    // Audio data starts after header (track + timestamp + flags)
    const audioStart = trackLen + 3;
    if (data.length <= audioStart) return null;

    // Extract raw audio data
    const audioData = data.slice(audioStart);

    return audioData;
  }

  /**
   * Read a variable-length integer (VINT) from buffer
   * @param buf Buffer to read from
   * @param offset Start offset
   * @param keepMarker If true, include marker bit in value (for element IDs)
   */
  readVINT(buf, offset, keepMarker) {
    if (offset >= buf.length) return null;

    const firstByte = buf[offset];
    let length = 1;
    let mask = 0x80;

    // Find the length by looking at leading bits
    while (length <= 8 && !(firstByte & mask)) {
      length++;
      mask >>= 1;
    }

    if (length > 8 || offset + length > buf.length) {
      return null;
    }

    let value = keepMarker ? firstByte : firstByte & (mask - 1);

    for (let i = 1; i < length; i++) {
      value = (value << 8) | buf[offset + i];
    }

    // Check for unknown size marker (all 1s after VINT marker)
    if (!keepMarker) {
      const maxVal = Math.pow(2, 7 * length) - 1;
      if (value === maxVal) {
        return { value: -1, length }; // Unknown size
      }
    }

    return { value, length };
  }
}

// ---------- DATABASE HELPERS ----------
async function getChannelForSession(callSessionId) {
  if (!callSessionId) return null;

  const client = await pgPool.connect();
  try {
    const res = await client.query(
      `SELECT asterisk_channel_id FROM call_sessions WHERE id = $1`,
      [callSessionId]
    );
    return res.rows[0]?.asterisk_channel_id || null;
  } catch (err) {
    console.error(`❌ DB error getting channel: ${err.message}`);
    return null;
  } finally {
    client.release();
  }
}

async function waitForChannelAttachment(callSessionId, attempts = 20, delayMs = 250) {
  for (let i = 0; i < attempts; i++) {
    const channelId = await getChannelForSession(callSessionId);
    if (channelId) {
      return channelId;
    }
    await wait(delayMs);
  }
  return null;
}

// ---------- SQS HELPERS ----------
async function sendTranscriptToQueue(payload) {
  if (!TRANSCRIPT_QUEUE_URL_GROUP) {
    console.warn("⚠️ No TRANSCRIPT_QUEUE_URL_GROUP configured");
    return;
  }

  try {
    const messageBody = {
      type: "transcript",
      transcript: payload.transcript,
      isPartial: payload.isPartial,
      callSessionId: payload.callSessionId,
      asteriskChannelId: payload.channelId,
      mipId: payload.mipId,
      meta: payload.meta,
      source: payload.source,
      timestamp: Date.now(),
    };

    const messageGroupId = `call-${payload.callSessionId}`;

    await sqs.send(
      new SendMessageCommand({
        QueueUrl: TRANSCRIPT_QUEUE_URL_GROUP,
        MessageBody: JSON.stringify(messageBody),
        MessageGroupId: messageGroupId,
        MessageDeduplicationId: `transcript-${payload.callSessionId}-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`,
      })
    );

    console.log(`📤 Sent ${payload.isPartial ? "partial" : "final"} transcript to SQS`);
  } catch (err) {
    console.error(`❌ SQS send error: ${err.message}`);
  }
}

async function sendBargeInToQueue(callSessionId, channelId, confidence) {
  if (!TRANSCRIPT_QUEUE_URL_GROUP) return;

  try {
    const messageBody = {
      type: "barge_in",
      asteriskChannelId: channelId,
      callSessionId,
      confidence,
      timestamp: Date.now(),
      source: isUltravoxMode() ? "ultravox" : "faster-whisper",
    };

    await sqs.send(
      new SendMessageCommand({
        QueueUrl: TRANSCRIPT_QUEUE_URL_GROUP,
        MessageBody: JSON.stringify(messageBody),
        MessageGroupId: `call-${callSessionId}`,
        MessageDeduplicationId: `bargein-${callSessionId}-${Date.now()}`,
      })
    );

    console.log(`📤 Sent barge-in notification to SQS`);
  } catch (err) {
    console.error(`❌ Failed to send barge-in: ${err.message}`);
  }
}

/**
 * Send Ultravox AI response to SQS for ARI Bridge to synthesize and play
 * This is for text responses that need TTS synthesis
 */
async function sendUltravoxResponseToQueue(payload) {
  if (!TRANSCRIPT_QUEUE_URL_GROUP) {
    console.warn("⚠️ No TRANSCRIPT_QUEUE_URL_GROUP configured");
    return;
  }

  try {
    const messageBody = {
      type: "ultravox_response",
      text: payload.text,
      callSessionId: payload.callSessionId,
      asteriskChannelId: payload.channelId,
      mipId: payload.mipId,
      meta: payload.meta,
      timestamp: Date.now(),
      source: "ultravox",
    };

    await sqs.send(
      new SendMessageCommand({
        QueueUrl: TRANSCRIPT_QUEUE_URL_GROUP,
        MessageBody: JSON.stringify(messageBody),
        MessageGroupId: `call-${payload.callSessionId}`,
        MessageDeduplicationId: `ultravox-resp-${payload.callSessionId}-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`,
      })
    );

    console.log(`📤 [ULTRAVOX] Sent AI response to SQS for TTS`);
  } catch (err) {
    console.error(`❌ Failed to send Ultravox response to SQS: ${err.message}`);
  }
}
