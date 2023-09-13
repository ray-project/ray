// Handlers for CSAT events

/**
 * Upvote or downvote the documentation via Google Analytics.
 *
 * @param {string} vote 'Yes' or 'No' vote to send as feedback
 */
function sendVote(vote) {
  gtag(
    'event',
    'Vote',
    {
      event_category: 'CSAT',
      event_label: vote,
      value: vote === 'Yes' ? 1 : 0
    }
  )
}

/**
 * Documentation feedback via Google Analytics
 * @param {string} text Text to send as feedback
 */
function sendFeedback(text) {
  gtag(
    'event',
    'Feedback',
    {
      event_category: 'CSAT',
      event_label: text,
    }
  )
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
