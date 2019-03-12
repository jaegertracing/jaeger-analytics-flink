# Jaeger Analytics 

## Introduction
This repository was created from the internal Uber repository used to run Flink jobs. It was created by stripping away Uber specific components, and hasn't been tested in it's current form. 
It is intended to serve as a starting point for a more generic OSS release. 

## Local development with IntelliJ

1. Open the the main (DependenciesJob, TraceQualityJob, etc) and hit the play/run button on the gutter. This will cause 
   IntelliJ to create a new run configuration and run the application. It will fail because it cannot find some classes.
2. Edit this run configuration by selecting **Run** -> **Edit Configurations**
3. Enable the ***Single Instance only** checkbox on the top left.
4. Enable the  **Include dependencies with the "Provided" scope** checkbox.
5. Run the job by hitting the play/run button. 

