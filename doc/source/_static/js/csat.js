// Handlers for CSAT events

/**
 * Upvote or downvote the documentation via Google Analytics.
 *
 * @param {string} vote 'Yes' or 'No' vote to send as feedback
 */
function sendVote(vote) {
  if (typeof window.dataLayer === 'undefined' || !Array.isArray(window.dataLayer)) {
    console.warn('Google Tag Manager dataLayer not available - CSAT vote not tracked');
    return;
  }

  window.dataLayer.push({
    event: 'csat_vote',
    vote_type: vote,
    category: 'CSAT',
    page_location: window.location.href,
    page_title: document.title,
    value: vote === 'Yes' ? 1 : 0
  });
}

/**
 * Documentation feedback via Google Analytics
 * @param {string} text Text to send as feedback
 */
function sendFeedback(text) {
  if (typeof window.dataLayer === 'undefined' || !Array.isArray(window.dataLayer)) {
    console.warn('Google Tag Manager dataLayer not available - CSAT feedback not tracked');
    return;
  }

  window.dataLayer.push({
    event: 'csat_feedback',
    feedback_text: text.substring(0, 500),
    category: 'CSAT',
    page_location: window.location.href,
    page_title: document.title,
    feedback_length: text.length
  });
}

window.addEventListener("DOMContentLoaded", () => {
  const yesButton = document.getElementById("csat-yes");
  const noButton = document.getElementById("csat-no");
  const yesIcon = document.getElementById("csat-yes-icon");
  const noIcon = document.getElementById("csat-no-icon");
  const text = document.getElementById("csat-textarea");
  const submitButton = document.getElementById("csat-submit");
  const csatGroup = document.getElementById("csat-textarea-group");
  const csatFeedbackReceived = document.getElementById("csat-feedback-received");
  const csatInputs = document.getElementById("csat-inputs")

  yesButton.addEventListener("click", () => {
    text.placeholder = "We're glad you found it helpful. If you have any additional feedback, please let us know.";
    csatGroup.classList.remove("csat-hidden");
    yesIcon.classList.remove("csat-hidden");
    noIcon.classList.add("csat-hidden");
    yesButton.classList.add("csat-button-active");
    noButton.classList.remove("csat-button-active");
    submitButton.scrollIntoView();
    sendVote('Yes');
  })

  noButton.addEventListener("click", () => {
    text.placeholder = "We value your feedback. Please let us know how we can improve our documentation.";
    csatGroup.classList.remove("csat-hidden");
    yesIcon.classList.add("csat-hidden");
    noIcon.classList.remove("csat-hidden");
    yesButton.classList.remove("csat-button-active");
    noButton.classList.add("csat-button-active");
    submitButton.scrollIntoView();
    sendVote('No');
  })

  submitButton.addEventListener("click", () => {
    csatGroup.classList.add("csat-hidden");
    csatInputs.classList.add("csat-hidden");
    csatFeedbackReceived.classList.remove("csat-hidden");
    sendFeedback(text.value);
  }, {once: true})
})
