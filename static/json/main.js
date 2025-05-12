// DOM elements
const form = document.getElementById('youtubeForm');
const loadingDiv = document.getElementById('loading');
const loadingOverlay = document.getElementById('loadingOverlay');
const resultDiv = document.getElementById('result');
const videoPreview = document.getElementById('videoPreview');
const noCommentsMessage = document.getElementById('noCommentsMessage');
const framesContainer = document.getElementById('framesContainer');
const noFramesMessage = document.getElementById('noFramesMessage');
const videoFramesResults = document.getElementById('video-frames-results');
const audioContainer = document.getElementById('audioContainer');
const noAudioMessage = document.getElementById('noAudioMessage');
const integratedResults = document.getElementById('integrated-results');

// Track processed frames to avoid duplicates
const processedFrames = new Set();
const processedAudioChunks = new Set();
let lastFrameCount = 0;

// Initialize intervals for updating
let updateInterval;
let framesUpdateInterval;
let audioUpdateInterval;
let integratedUpdateInterval;

// Function to extract YouTube video ID from URL
function getYoutubeId(url) {
    const regExp = /^.*(youtu.be\/|v\/|u\/\w\/|embed\/|watch\?v=|\&v=)([^#\&\?]*).*/;
    const match = url.match(regExp);
    return (match && match[2].length === 11) ? match[2] : null;
}

// Initialize chart if results exist
function initializeChart() {
    const ctx = document.getElementById('sentimentChart').getContext('2d');
    const totalComments = window.resultData ? window.resultData.total : 0;

    if (totalComments > 0) {
        window.sentimentChart = new Chart(ctx, {
            type: 'bar',
            data: {
                labels: ['Positive', 'Negative', 'Neutral'],
                datasets: [{
                    label: 'Comment Sentiment Distribution',
                    data: [
                        window.resultData.positive || 0,
                        window.resultData.negative || 0,
                        window.resultData.neutral || 0
                    ],
                    backgroundColor: ['#4ade80', '#f87171', '#fbbf24'],
                    borderColor: ['#4ade80', '#f87171', '#fbbf24'],
                    borderWidth: 1,
                    borderRadius: 8
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: {
                        display: false
                    },
                    tooltip: {
                        backgroundColor: 'rgba(0, 0, 0, 0.8)',
                        padding: 12,
                        titleFont: {
                            size: 14,
                            weight: 'bold'
                        },
                        bodyFont: {
                            size: 14
                        },
                        callbacks: {
                            label: function(context) {
                                let label = context.dataset.label || '';
                                if (label) {
                                    label += ': ';
                                }
                                if (context.parsed.y !== null) {
                                    const total = totalComments || 1;
                                    const percentage = Math.round((context.parsed.y / total) * 100);
                                    label += `${context.parsed.y} comments (${percentage}%)`;
                                }
                                return label;
                            }
                        }
                    }
                },
                scales: {
                    y: {
                        beginAtZero: true,
                        title: {
                            display: true,
                            text: 'Number of Comments',
                            font: {
                                size: 14,
                                weight: 'bold'
                            }
                        },
                        ticks: {
                            precision: 0
                        }
                    },
                    x: {
                        title: {
                            display: true,
                            text: 'Sentiment Category',
                            font: {
                                size: 14,
                                weight: 'bold'
                            }
                        }
                    }
                }
            }
        });

        // Show results and hide no comments message
        resultDiv.style.display = 'block';
        noCommentsMessage.style.display = 'none';
    } else {
        // Show no comments message and hide results
        resultDiv.style.display = 'none';
        noCommentsMessage.style.display = 'block';
    }
}

// Helper function to create emotion badges
function createEmotionBadges(emotions) {
    let badgesHTML = '';

    if (!emotions) return badgesHTML;

    // Map emotion names to appropriate classes
    const emotionClasses = {
        'happy': 'happy',
        'sad': 'sad',
        'angry': 'angry',
        'surprise': 'surprise',
        'fear': 'fear',
        'neutral': 'neutral',
        'disgust': 'disgusted'
    };

    // Process each emotion in the object
    for (const [emotion, value] of Object.entries(emotions)) {
        if (value > 0) {
            const className = emotionClasses[emotion.toLowerCase()] || 'neutral';
            badgesHTML += `<span class="emotion-badge ${className}">${emotion}: ${value}</span>`;
        }
    }

    return badgesHTML;
}

// Function to create a frame card
function createFrameCard(frame, isNew = false) {
    return `
        <div class="frame-card ${isNew ? 'new' : ''}" id="frame-${frame.frame_id}">
            <div class="frame-header">
                <div class="frame-id">Frame #${frame.frame_id}</div>
                <div class="frame-time">
                    <i class="far fa-clock me-1"></i> Processed at: ${frame.processed_at}
                </div>
            </div>
            <div class="frame-details">
                <div class="frame-faces">
                    <i class="fas fa-user faces-icon"></i>
                    <span>${frame.num_faces} face${frame.num_faces !== 1 ? 's' : ''} detected</span>
                </div>
                <div class="emotions-badges">
                    ${createEmotionBadges(frame.emotions)}
                </div>
            </div>
        </div>
    `;
}

// Mapping emotion names to CSS classes
function mapEmotionToClass(emotion) {
    const map = {
        'happy': 'happy',
        'joy': 'happy',
        'sad': 'sad',
        'sadness': 'sad',
        'angry': 'angry',
        'anger': 'angry',
        'surprise': 'surprise',
        'fear': 'fear',
        'neutral': 'neutral',
        'disgust': 'disgusted'
    };

    return map[emotion.toLowerCase()] || 'neutral';
}

// Function to create an audio card
function createAudioCard(audio, isNew = false) {
    // Create badges for sentiment
    let sentimentBadges = '';
    if (audio.sentiment) {
        for (const [sentiment, value] of Object.entries(audio.sentiment)) {
            if (value > 0) {
                const className = `sentiment-${sentiment.toLowerCase()}`;
                sentimentBadges += `<span class="emotion-badge ${className}">${sentiment}: ${value}</span>`;
            }
        }
    }

    // Create badges for emotion
    let emotionBadges = '';
    if (audio.emotion) {
        for (const [emotion, value] of Object.entries(audio.emotion)) {
            if (value > 0) {
                // Reuse classes from video emotions
                const emotionClass = mapEmotionToClass(emotion);
                emotionBadges += `<span class="emotion-badge ${emotionClass}">${emotion}: ${value}</span>`;
            }
        }
    }

    return `
        <div class="audio-card ${isNew ? 'new' : ''}" id="audio-${audio.chunk_id}">
            <div class="audio-header">
                <div class="chunk-id">Chunk #${audio.chunk_id}</div>
                <div class="audio-time">
                    <i class="far fa-clock me-1"></i> Processed at: ${audio.processed_at}
                </div>
            </div>
            <div class="audio-text">"${audio.text}"</div>
            <div class="frame-details">
                <div class="emotions-badges">
                    ${sentimentBadges}
                    ${emotionBadges}
                </div>
            </div>
        </div>
    `;
}

// Function to update video frames results
function updateVideoFrames() {
    fetch('/api/get_video_sentiments')
        .then(response => response.json())
        .then(data => {
            if (data && data.length > 0) {
                // Hide the no frames message
                noFramesMessage.style.display = 'none';
                videoFramesResults.style.display = 'block';

                // Check if we have new frames
                const newFrames = data.filter(frame => !processedFrames.has(frame.frame_id));

                if (newFrames.length > 0) {
                    // Update our set of processed frames
                    newFrames.forEach(frame => processedFrames.add(frame.frame_id));

                    // Sort newest first
                    newFrames.sort((a, b) => {
                        return new Date(b.processed_at) - new Date(a.processed_at);
                    });

                    // Create HTML for new frames
                    let newFramesHTML = '';
                    newFrames.forEach(frame => {
                        newFramesHTML += createFrameCard(frame, true);
                    });

                    // Insert at the beginning of the container
                    if (framesContainer.innerHTML === noFramesMessage.outerHTML) {
                        framesContainer.innerHTML = newFramesHTML;
                    } else {
                        framesContainer.insertAdjacentHTML('afterbegin', newFramesHTML);
                    }

                    // After a second, remove the 'new' class from the new cards
                    setTimeout(() => {
                        newFrames.forEach(frame => {
                            const frameElement = document.getElementById(`frame-${frame.frame_id}`);
                            if (frameElement) {
                                frameElement.classList.remove('new');
                            }
                        });
                    }, 1000);
                }
            }
        })
        .catch(error => console.error('Error updating video frames:', error));
}

// Function to update comment sentiment data
function updateSentimentData() {
    fetch('/api/get_results')
        .then(response => response.json())
        .then(data => {
            // Hide loading overlay once we have data
            loadingOverlay.style.display = 'none';
            
            // Store data globally
            window.resultData = data;

            // Update chart if it exists
            if (window.sentimentChart) {
                window.sentimentChart.data.datasets[0].data = [
                    data.positive,
                    data.negative,
                    data.neutral
                ];
                window.sentimentChart.update();
            }

            // Update percentages
            const total = data.positive + data.negative + data.neutral;
            if (total > 0) {
                document.getElementById('positivePercentage').textContent =
                    Math.round((data.positive / total) * 100) + '%';
                document.getElementById('negativePercentage').textContent =
                    Math.round((data.negative / total) * 100) + '%';
                document.getElementById('neutralPercentage').textContent =
                    Math.round((data.neutral / total) * 100) + '%';

                // Show results if we have data
                resultDiv.style.display = 'block';
                noCommentsMessage.style.display = 'none';
            } else {
                // No comments yet
                resultDiv.style.display = 'none';
                noCommentsMessage.style.display = 'block';
            }
        })
        .catch(error => console.error('Error updating sentiment data:', error));
}

// Function to update audio results
function updateAudioResults() {
    fetch('/api/get_audio_sentiments')
        .then(response => response.json())
        .then(data => {
            if (data && data.length > 0) {
                // Hide the no audio message
                document.getElementById('noAudioMessage').style.display = 'none';
                document.getElementById('audio-results').style.display = 'block';

                // Check if we have new audio chunks
                const newAudioChunks = data.filter(chunk => !processedAudioChunks.has(chunk.chunk_id));

                if (newAudioChunks.length > 0) {
                    // Update our set of processed chunks
                    newAudioChunks.forEach(chunk => processedAudioChunks.add(chunk.chunk_id));

                    // Sort newest first
                    newAudioChunks.sort((a, b) => {
                        return new Date(b.processed_at) - new Date(a.processed_at);
                    });

                    // Create HTML for new chunks
                    let newAudioHTML = '';
                    newAudioChunks.forEach(chunk => {
                        newAudioHTML += createAudioCard(chunk, true);
                    });

                    // Insert at the beginning of the container
                    if (audioContainer.innerHTML === noAudioMessage.outerHTML) {
                        audioContainer.innerHTML = newAudioHTML;
                    } else {
                        audioContainer.insertAdjacentHTML('afterbegin', newAudioHTML);
                    }

                    // After a second, remove the 'new' class
                    setTimeout(() => {
                        newAudioChunks.forEach(chunk => {
                            const audioElement = document.getElementById(`audio-${chunk.chunk_id}`);
                            if (audioElement) {
                                audioElement.classList.remove('new');
                            }
                        });
                    }, 1000);
                }
            }
        })
        .catch(error => console.error('Error updating audio results:', error));
}

// Function to update integrated results
function updateIntegratedResults() {
    fetch('/api/get_integrated_results')
        .then(response => response.json())
        .then(data => {
            if (data) {
                // Show the integrated results section
                integratedResults.style.display = 'block';
                
                // Update overall sentiment score
                document.getElementById('overallSentimentScore').textContent = 
                    data.overall_sentiment_score.toFixed(2);
                
                // Update overall emotion
                document.getElementById('dominantEmotion').textContent = 
                    data.dominant_emotion || 'Neutral';
                
                // Update the integrated charts
                updateIntegratedCharts(data);
            }
        })
        .catch(error => console.error('Error updating integrated results:', error));
}

// Function to initialize integrated charts
function updateIntegratedCharts(data) {
    // Update or create video emotions chart
    if (window.videoEmotionsChart) {
        window.videoEmotionsChart.data.datasets[0].data = Object.values(data.video_emotions || {});
        window.videoEmotionsChart.update();
    } else if (data.video_emotions) {
        const videoCtx = document.getElementById('videoEmotionsChart').getContext('2d');
        window.videoEmotionsChart = new Chart(videoCtx, {
            type: 'doughnut',
            data: {
                labels: Object.keys(data.video_emotions),
                datasets: [{
                    data: Object.values(data.video_emotions),
                    backgroundColor: [
                        '#4ade80', // happy
                        '#60a5fa', // sad
                        '#f87171', // angry
                        '#fbbf24', // surprise
                        '#a78bfa', // fear
                        '#9ca3af', // neutral
                        '#10b981'  // disgust
                    ],
                    borderWidth: 1
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: {
                        position: 'right',
                        labels: {
                            font: {
                                size: 12
                            }
                        }