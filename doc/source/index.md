```{include} /_includes/overview/announcement.md
```

```{raw} html

<script>
    const toc = document.getElementsByClassName("bd-toc")[0];
    toc.style.cssText = 'display: none !important';
    const main = document.getElementById("main-content");
    main.style.cssText = 'max-width: 100% !important'; 
</script>

<title>Welcome to Ray!</title>


<div class="container">
  <div class="row">
    <div class="col-6">
      <h1>Welcome to Ray!</h1>
      <p>Ray is an open-source unified framework for scaling AI and Python applications. 
      It provides the compute layer for parallel processing so that 
      you don’t need to be a distributed systems expert.
      </p>
      <div class="image-header">
        <a href="https://github.com/ray-project/ray">
            <img src="/_static/img/github-fill.png" width="25px" height="25px" />
        </a>
        <a href="https://docs.google.com/forms/d/e/1FAIpQLSfAcoiLCHOguOm8e7Jnn-JJdZaCxPGjgVCvFijHB5PLaQLeig/viewform">
            <img src="/_static/img/slack-fill.png" width="25px" height="25px" />
        </a>
        <a href="https://twitter.com/raydistributed">
            <img src="/_static/img/twitter-fill.png" width="25px" height="25px" />
        </a>
      </div>
    </div>
    <div class="col-6">
      <iframe width="450px" height="240px" style="border-radius:"10px";"
       src="https://www.youtube.com/embed/Iu_F_EzQb5o" 
       title="YouTube Preview" 
       frameborder="0" allow="accelerometer; autoplay;" 
       allowfullscreen></iframe>
    </div>
  </div>
</div>

<div class="container">
    <h2>Getting Started</h2>
<div class="container">

<div class="container">
  <div class="row">
    <div class="col-4 info-box">
        <div class="image-header">
            <img src="/_static/img/ray_logo.png" width="30px" height="30px" />
            <h3>Learn basics</h3>
        </div>
        <p>Understand how the Ray framework scales your ML workflows.</p>      
        <a class="bold-link" href="./ray-overview/index.html" >Learn more ></a>      
    </div>
    <div class="col-4 info-box">
        <div class="image-header">
            <img src="/_static/img/download.png" width="30px" height="30px" />
            <h3>Install Ray</h3>
        </div>
        <p><pre><code class="language-bash" style="margin:10px;">pip install "ray[default]"</code></pre></p>      
        <a class="bold-link" href="./ray-overview/installation.html" >Installation guide ></a>      
    </div>
    <div class="col-4 info-box">
        <div class="image-header">
            <img src="/_static/img/code.png" width="30px" height="30px" />
            <h3>Try it out</h3>
        </div>
        <p>Experiment with Ray with an introductory notebook.</p>
        <a class="bold-link" href="https://colab.research.google.com/github/maxpumperla/learning_ray/blob/main/notebooks/ch_02_ray_core.ipynb" 
        >Open the notebook></a>      
    </div>
  </div>
</div>


<div class="container">

<h2>Scaling with Ray</h2>

<div class="row">
    <div class="col-4">
        <div class="nav flex-column nav-pills" id="v-pills-tab" role="tablist" aria-orientation="vertical">
          <a class="nav-link active" id="v-pills-home-tab" data-toggle="pill" href="#v-pills-home" role="tab" aria-controls="v-pills-home" aria-selected="true">Home</a>
          <a class="nav-link" id="v-pills-profile-tab" data-toggle="pill" href="#v-pills-profile" role="tab" aria-controls="v-pills-profile" aria-selected="false">Profile</a>
          <a class="nav-link" id="v-pills-messages-tab" data-toggle="pill" href="#v-pills-messages" role="tab" aria-controls="v-pills-messages" aria-selected="false">Messages</a>
          <a class="nav-link" id="v-pills-settings-tab" data-toggle="pill" href="#v-pills-settings" role="tab" aria-controls="v-pills-settings" aria-selected="false">Settings</a>
        </div>
    </div>
    <div class="col-8">
        <div class="tab-content" id="v-pills-tabContent">
          <div class="tab-pane fade show active" id="v-pills-home" role="tabpanel" aria-labelledby="v-pills-home-tab">
            Foo
          </div>
          <div class="tab-pane fade" id="v-pills-profile" role="tabpanel" aria-labelledby="v-pills-profile-tab">Bar</div>
          <div class="tab-pane fade" id="v-pills-messages" role="tabpanel" aria-labelledby="v-pills-messages-tab">FooBar</div>
          <div class="tab-pane fade" id="v-pills-settings" role="tabpanel" aria-labelledby="v-pills-settings-tab">Baz</div>
        </div>
    </div>
</div>
  
</div>

<div class="container">
    <h2>Beyond the basics</h2>
</div>

<div class="container">
  <div class="row">
    <div class="col-6 info-box">
        <div class="image-header">
            <img src="/_static/img/AIR.png" width="30px" height="30px" />
            <h3>Ray AI Runtime</h3>
        </div>
        <p>Scale the entire ML pipeline from data ingest to model serving with high-level Python APIs that integrate with popular ecosystem frameworks.</p>      
        <a class="bold-link" href="./ray-air/getting-started.html" >Learn more about AIR></a>      
    </div>
    <div class="col-6 info-box">
        <div class="image-header">
            <img src="/_static/img/Core.png" width="30px" height="30px" />
            <h3>Ray Core</h3>
        </div>
        <p>Scale generic Python code with simple, foundational primitives that enable a high degree of control for building distributed applications or custom platforms.</p>
        <a class="bold-link" href="./ray-overview/installation.html" >Learn more about Core ></a>      
    </div>
  </div>
</div>



<div class="container">
    <h2>Getting involved</h2>
<div class="container">


<div class="container">
  
  <div class="row">
    <div class="col-4">
        <h4>Join the community</h4> 
    </div>
    <div class="col-4">
        <h4>Get support</h4>
    </div>
    <div class="col-4">
        <h4>Contribute to Ray</h4>
    </div>
  </div>
  
  <div class="row">
    <div class="col-4 community-box">
        <div class="image-header">
            <img src="/_static/img/meetup.png" width="20px" height="20px" />
            <p>Attend community events</p>
        </div>    
    </div>
    <div class="col-4 community-box">
        <div class="image-header">
            <img src="/_static/img/slack-fill.png" width="20px" height="20px" />
            <p>Find community on Slack</p>
        </div>    
    </div>
    <div class="col-4 community-box">
        <a href="./" class="image-header">
            <img src="/_static/img/pen.png" width="20px" height="20px" />
            <p>Contributor guide</p>
        </a>    
    </div>
  </div>

</div>


```
