from transformers import WhisperProcessor, WhisperForConditionalGeneration
import torchaudio

# Load processor and model
processor = WhisperProcessor.from_pretrained("vinai/PhoWhisper-large")
model = WhisperForConditionalGeneration.from_pretrained("vinai/PhoWhisper-large")

# Load and preprocess audio
waveform, sample_rate = torchaudio.load("bomman-gay_CQBLyhx.wav")
if sample_rate != 16000:
    resampler = torchaudio.transforms.Resample(orig_freq=sample_rate, new_freq=16000)
    waveform = resampler(waveform)
waveform = waveform[0]  # Convert to mono

# Prepare input
inputs = processor(
    waveform,
    sampling_rate=16000,
    return_tensors="pt",
    language="vi",
    return_attention_mask=True
)

# Generate transcription
forced_decoder_ids = processor.get_decoder_prompt_ids(language="vi", task="transcribe")
predicted_ids = model.generate(
    inputs.input_features,
    attention_mask=inputs.attention_mask,
    forced_decoder_ids=forced_decoder_ids,
    suppress_tokens=None  # Ensure no token suppression
)

# Decode transcription without filtering
transcription = processor.batch_decode(predicted_ids, skip_special_tokens=True)[0]
print("Kết quả gốc:", transcription)