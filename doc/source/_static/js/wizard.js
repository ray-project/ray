const trainDescriptions = {
    torch: "Select PyTorch if you are using a custom PyTorch training loop. You can also use it to " +
        "train PyTorchLightning models or Huggingface transformer or diffuser models.",
    tf: "Select TensorFlow if you are using a custom tensorflow training loop. " +
        "This can also be a loop that utilizes Keras for training.",
    xgboost: "Select XGBoost if you are using a custom XGBoost training loop.",
}

const dataDescriptions = {
    nativedata: "Select this if you're e.g. using Pandas, a " +
        "PyTorch dataloader or TensorFlow dataset to load your data.",
    raydata: "Select this if you're not using any of the native dataloaders, " +
        "yet, or if you'd like to use Ray Data to preprocess you data.",
}

const dataTypeDescriptions = {
    any: "Just give me an example with any kind of data.",
    image: "Use image data for the example, fon instance for a computer vision task.",
    text: "Use text data for the example, for instance for a NLP task.",
}

function unsetExample() {
    const exampleSelectionDesc = document.getElementById("exampleSelectionDesc");
    exampleSelectionDesc.innerText = "";

    const wizardCode = document.getElementById("wizardCode");
    wizardCode.innerHTML = "";

    const doMoreDesc = document.getElementById("doMoreDesc");
    doMoreDesc.innerText = "";

    const doMoreCode = document.getElementById("doMoreCode");
    doMoreCode.innerHTML = "";
}


window.addEventListener("load", () => {

    const basicSelection = document.getElementById("wizardMain");
    const doMoreSelection = document.getElementById("doMoreSelection");

    if (basicSelection) {

        const wizardMainForm = document.getElementById("wizardMainForm");
        const wizardDoMoreForm = document.getElementById("wizardDoMoreForm");

        wizardMainForm.reset();
        wizardDoMoreForm.reset();

        const trainRadios = document.getElementsByName("trainGroup");
        const dataSelection = document.getElementById("dataSelection");
        const trainDesc = document.getElementById("trainDesc");

        let trainTag = "";
        trainRadios.forEach(radio => {
            radio.addEventListener("change", function () {
                if (this.checked) {
                    dataSelection.style.cssText = 'display: normal !important'
                    trainDesc.innerText = trainDescriptions[this.value];
                    trainTag = this.value;
                }
                unsetExample();
                wizardDoMoreForm.reset();
                doMoreSelection.style.cssText = 'display: none !important'
            });
        });

        const dataRadios = document.getElementsByName("dataGroup");
        const dataTypeSelection = document.getElementById("dataTypeSelection");
        const dataDesc = document.getElementById("dataDesc");


        let dataTag = "";
        dataRadios.forEach(radio => {
            radio.addEventListener("change", function () {
                if (this.checked) {
                    dataTypeSelection.style.cssText = 'display: normal !important'
                    dataDesc.innerText = dataDescriptions[this.value];
                    dataTag = this.value;
                }
                unsetExample();
                wizardDoMoreForm.reset();
                doMoreSelection.style.cssText = 'display: none !important'
            });
        });

        const dataTypeRadios = document.getElementsByName("dataTypeGroup");
        const submitButton = document.getElementById('generateButton');
        const dataTypeDesc = document.getElementById("dataTypeDesc");

        let dataTypeTag = "";
        dataTypeRadios.forEach(radio => {
            radio.addEventListener("change", function () {
                if (this.checked) {
                    // generateButton.style.cssText = 'display: flex !important'
                    dataTypeDesc.innerText = dataTypeDescriptions[this.value];
                    dataTypeTag = this.value;
                }
                unsetExample();
                wizardDoMoreForm.reset();
                doMoreSelection.style.cssText = 'display: none !important'
                loadExample();
            });
        });

        function loadExample() {
            const pageUrl = window.location.href
            const res = pageUrl.split("/");

            let baseUrl = "";
            if (res[2] === "docs.ray.io") {
                baseUrl = "https://docs.ray.io/en/" + res[4];
            }

            const example = baseUrl + "/wizard/" + trainTag + "_"
                + dataTag + "_" + dataTypeTag + ".html";

            fetch(example)
                .then(response => response.text())
                .then(html => {
                    const parser = new DOMParser();
                    const doc = parser.parseFromString(html, 'text/html');
                    const requirementBlocks = doc.getElementsByClassName("highlight-default")[0];
                    const codeBlocks = doc.getElementsByClassName("highlight-default")[1];
                    const header = doc.getElementsByTagName("h1")[1];

                    const exampleSelectionDesc = document.getElementById("exampleSelectionDesc");
                    exampleSelectionDesc.innerText = "Check out our example: " + header.innerText.trim();

                    const wizardCode = document.getElementById("wizardCode");

                    wizardCode.innerHTML = requirementBlocks.innerHTML;
                    wizardCode.innerHTML += codeBlocks.innerHTML;

                    doMoreSelection.style.cssText = 'display: normal !important'
                })
                .catch(error => {
                    console.log(error);

                    const exampleSelectionDesc = document.getElementById("exampleSelectionDesc");
                    exampleSelectionDesc.innerText = "Unfortunately, this example could not be found.";

                    const wizardCode = document.getElementById("wizardCode");
                    wizardCode.innerHTML = "";
                });
        };

        //        submitButton.addEventListener('click', function (event) {
        //
        //        });

        function loadDoMore() {
            const pageUrl = window.location.href
            const res = pageUrl.split("/");

            let baseUrl = "";
            if (res[2] === "docs.ray.io") {
                baseUrl = "https://docs.ray.io/en/" + res[4];
            }

            const example = baseUrl + "/wizard/" + trainTag + "_"
                + moreTag + "_" + dataTypeTag + ".html";

            fetch(example)
                .then(response => response.text())
                .then(html => {
                    const parser = new DOMParser();
                    const doc = parser.parseFromString(html, 'text/html');
                    const code = doc.getElementsByClassName("highlight-default")[0];
                    const header = doc.getElementsByTagName("h1")[1];

                    const doMoreDesc = document.getElementById("doMoreDesc");
                    doMoreDesc.innerText = header.innerText.trim();

                    const doMoreCode = document.getElementById("doMoreCode");
                    doMoreCode.innerHTML = code.innerHTML;
                })
                .catch(error => {
                    console.log(error);
                });
        };


        const doMoreRadios = document.getElementsByName("doMoreGroup");
        let moreTag = "";
        doMoreRadios.forEach(radio => {
            radio.addEventListener("change", function () {
                if (this.checked) {
                    moreTag = this.value;
                    loadDoMore();
                }
            });
        });

    }


});
