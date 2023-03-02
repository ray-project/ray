.. _examples-wizard:

Example Wizard
==============

You can use this wizard to create a Ray example that suits your needs.
Simply start picking options from the list below and we'll guide you to your example.

.. raw:: html

    <p>First, pick one or multiple parts of an ML workload:</p>

    <div id="basicSelection">
        <div type="button" id="dataWizard" class="btn btn-outline-primary">Data Preprocessing</div>
        <div type="button" id="trainWizard" class=" btn btn-outline-primary">Model Training</div>
        <div type="button" id="tuneWizard" class="btn btn-outline-primary">Hyperparameter Tuning</div>
        <div type="button" id="predWizard" class="btn btn-outline-primary">Getting Predictions</div>
    </div>

    <p id="instructions" style="display:none;">
        Next, select what you want to do with each part of the workload:
    </p>

    <div class="wizardContainer">
    <div id="dataSelection" class="btn-group" role="group" style="display:none;">
        <div class="form-check form-check-inline">
          <input class="form-check-input" type="radio" name="dataGroup" id="basicData" value="basicData">
          <label class="form-check-label" for="basicData">Basic</label>

        <div class="form-check form-check-inline">
          <input class="form-check-input" type="radio" name="dataGroup" id="pytorchData" value="pytorchData">
          <label class="form-check-label" for="pytorchData">PyTorch</label>
        </div>
        <div class="form-check form-check-inline">
          <input class="form-check-input" type="radio" name="dataGroup" id="tfData" value="tfData">
          <label class="form-check-label" for="tfData">TensorFlow</label>
        </div>
    </div>
    </div>

    <div class="wizardContainer">
    <div id="trainSelection" class="btn-group" role="group" style="display:none;">
        <div class="form-check form-check-inline">
          <input class="form-check-input" type="radio" name="trainGroup" id="pytorchTrain" value="pytorchTrain">
          <label class="form-check-label" for="pytorchTrain">PyTorch</label>
        </div>
        <div class="form-check form-check-inline">
          <input class="form-check-input" type="radio" name="trainGroup" id="tfTrain" value="tfTrain">
          <label class="form-check-label" for="tfTrain">TensorFlow</label>
        </div>
    </div>
    </div>

    <div class="wizardContainer">
    <div id="tuneSelection" class="btn-group" role="group" style="display:none;">
        <div class="form-check form-check-inline">
          <input class="form-check-input" type="radio" name="tuneGroup" id="randomSearch" value="randomSearch">
          <label class="form-check-label" for="randomSearch">Random Search</label>
        </div>
        <div class="form-check form-check-inline">
          <input class="form-check-input" type="radio" name="tuneGroup" id="optunaSearch" value="optunaSearch">
          <label class="form-check-label" for="optunaSearch">Optuna</label>
        </div>
        <div class="form-check form-check-inline">
          <input class="form-check-input" type="radio" name="tuneGroup" id="hyperoptSearch" value="hyperoptSearch">
          <label class="form-check-label" for="hyperoptSearch">Hyperopt</label>
        </div>
    </div>
    </div>

    <div class="wizardContainer">
    <div id="predSelection" class="btn-group" role="group" style="display:none;">
        <div class="form-check form-check-inline">
          <input class="form-check-input" type="radio" name="predGroup" id="batchPrediction" value="batchPrediction">
          <label class="form-check-label" for="batchPrediction">Batch Prediction</label>
        </div>
        <div class="form-check form-check-inline">
          <input class="form-check-input" type="radio" name="predGroup" id="modelServing" value="modelServing">
          <label class="form-check-label" for="modelServing">Model Serving</label>
        </div>
    </div>
    </div>

    <div class="wizardContainer">
        <p>We detected that you want to use multiple components. Do you want to use the Ray AI Runtime (AIR) as umbrella for other Ray libraries?
        </p>
    </div>
    <div class="wizardContainer">
        <div id="airSelection" class="btn-group" role="group" style="display:none;">
            <div class="form-check form-check-inline">
              <input class="form-check-input" type="radio" name="airGroup" id="useAir" value="useAir">
              <label class="form-check-label" for="useAir">Use Ray AIR</label>
            </div>
            <div class="form-check form-check-inline">
              <input class="form-check-input" type="radio" name="airGroup" id="noAir" value="noAir">
              <label class="form-check-label" for="noAir">Use Ray Libraries in Isolation</label>
            </div>
        </div>
    </div>

    <div class="wizardContainer">
    <div id="generateButton" type="button" class="btn btn-primary" style="display:none;">
        Generate Example
    </div>
    </div>

    <div class="wizardContainer">
        <div id="wizardCode">
        </div>
    </div>
