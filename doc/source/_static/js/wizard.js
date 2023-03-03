let useTrain = false;
let useTune = false;
let usePredict = false;
let useData= false;
let anyChecked = false;
let multipleComponents = false;

//import wizard_flow from './wizard_flow.json';

let mapping = {
  start: {
    title: "Select training framework",
    options: [
      {
        name: "PyTorch",
        tags: [
          "pytorch"
        ],
        description: "Select PyTorch if you are using a custom PyTorch training loop. You can also use it to train PyTorchLightning models or Huggingface transformer/diffuser models.",
        next: "data"
      },
      {
        name: "TensorFlow",
        tags: [
          "tensorflow"
        ],
        description: "Select TensorFlow if you are using a custom tensorflow training loop. This can also be a loop that utilizes Keras for training.",
        next: "data"
      }
    ]
  },
  data: {
    title: "How do you load your data?",
    options: [
      {
        name: "I already have a way to load data",
        tags: [
          "native-data"
        ],
        description: "Select this if you're e.g. using Pandas, a PyTorch dataloader or TensorFlow dataset to load your data.",
        next: "task"
      },
      {
        name: "I still need to implement data loading (let's use Ray Data)\n",
        tags: [
          "ray-data"
        ],
        description: "Select this if you're not using any of the native dataloaders, yet, or if you'd like to use Ray Data to preprocess you data.",
        next: "task"
      }
    ]
  },
  task: {
    title: "What kind of data are you training on?",
    options: [
      {
        name: "Image data (CV)",
        tags: [
          "image"
        ],
        description: "",
        next: "finish"
      },
      {
        name: "Text data (NLP)",
        tags: [
          "nlp"
        ],
        description: "",
        next: "finish"
      }
    ]
  }
}

function toggleCheck(button) {

    if (button.classList.contains('btn-primary')) {
        button.classList.replace('btn-primary', 'btn-outline-primary');
    } else {
        button.classList.replace('btn-outline-primary', 'btn-primary');
    }
}

function toggleVisibility(section) {
    if (section.style.cssText.includes('none')) {
        section.style.cssText = 'display: flex !important'
    } else {
        section.style.cssText = 'display: none !important'
    }

    const generateButton = document.getElementById('generateButton');
    const instructions = document.getElementById('instructions');
    const airSelection = document.getElementById('airSelection');
    const airInstructions = document.getElementById('airInstructions');

    anyChecked = useTrain || useTune || usePredict || useData;
    multipleComponents = useTrain + useTune + usePredict + useData > 1;

    if (anyChecked) {
        generateButton.style.cssText = 'display: flex !important'
        instructions.style.cssText = 'display: flex !important'
    } else {
        generateButton.style.cssText = 'display: none !important'
        instructions.style.cssText = 'display: none !important'
    }

    if (multipleComponents) {
        airSelection.style.cssText = 'display: flex !important'
        airInstructions.style.cssText = 'display: flex !important'

    } else {
        airSelection.style.cssText = 'display: none !important'
        airInstructions.style.cssText = 'display: none !important'
    }
}

window.addEventListener("load", () => {

    const basicSelection = document.getElementById("basicSelection");
    if (basicSelection) {

        const data = document.getElementById("dataWizard");
        const dataSelection = document.getElementById("dataSelection");

        data.addEventListener("click", () => {
            useData = ! useData;
            toggleCheck(data);
            toggleVisibility(dataSelection);
        });

        const train = document.getElementById("trainWizard");
        const trainSelection = document.getElementById("trainSelection");

        train.addEventListener("click", () => {
            useTrain = ! useTrain;
            toggleCheck(train);
            toggleVisibility(trainSelection);
        });

        const tune = document.getElementById("tuneWizard");
        const tuneSelection = document.getElementById("tuneSelection");

        tune.addEventListener("click", () => {
            useTune = ! useTune;
            toggleCheck(tune);
            toggleVisibility(tuneSelection);
        });

        const pred = document.getElementById("predWizard");
        const predSelection = document.getElementById("predSelection");

        pred.addEventListener("click", () => {
            usePredict = ! usePredict;
            toggleCheck(pred);
            toggleVisibility(predSelection);
        });

    }

    // Render first step

    const submitButton = document.getElementById('generateButton');
    submitButton.addEventListener('click', function (event) {
          const pageUrl = window.location.href
        const res = pageUrl.split("/");

        let baseUrl = "";
        if (res[2] === "docs.ray.io") {
            baseUrl = "https://docs.ray.io/en/" +  res[4];
        }

        // TODO Select the right example here:
        let example = baseUrl + "/wizard/tune_test.html";

        fetch(example)
          .then(response => response.text())
          .then(html => {
            const parser = new DOMParser();
            const doc = parser.parseFromString(html, 'text/html');
            const code = doc.getElementsByClassName("highlight-default")[0];

            const wizardCode = document.getElementById("wizardCode");
            wizardCode.innerHTML = code.innerHTML;
          })
          .catch(error => console.log(error));
        });



});



