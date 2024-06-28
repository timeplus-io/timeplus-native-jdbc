Contributing Guide
==================
We welcome anyone that wants to help out in any way, whether that includes reporting problems, helping with documentations, or contributing code changes to fix bugs, add tests, or implement new features. This document outlines the basic steps required to work with and contribute to the codebase.

### Create issue

You can report problems or request features by creating [GitHub Issues](https://github.com/timeplus-io/timeplus-native-jdbc/issues).

### Install the tools

The following software is required to work with the codebase and build it locally:

* Git
* JDK 8/11
* Maven
* Docker

You can verify the tools are installed and running:

    git --version
    javac -version
    mvn -version
    docker --version

### GitHub account

If you don't already have a GitHub account you'll need to [join](https://github.com/join).

### Fork the repository

Go to the [timeplus-native-jdbc repository](https://github.com/timeplus-io/timeplus-native-jdbc) and press the "Fork" button near the upper right corner of the page. When finished, you will have your own "fork" at `https://github.com/<your-username>/timeplus-native-jdbc`, and this is the repository to which you will upload your proposed changes and create pull requests. For details, see the [GitHub documentation](https://help.github.com/articles/fork-a-repo/).

### Clone your fork

At a terminal, go to the directory in which you want to place a local clone of the timeplus-native-jdbc repository, and run the following commands to use HTTPS authentication:

    git clone https://github.com/<your-username>/timeplus-native-jdbc.git

If you prefer to use SSH and have [uploaded your public key to your GitHub account](https://help.github.com/articles/adding-a-new-ssh-key-to-your-github-account/), you can instead use SSH:

    git clone git@github.com:<your-username>/timeplus-native-jdbc.git

This will create a `timeplus-native-jdbc` directory, so change into that directory:

    cd timeplus-native-jdbc

This repository knows about your fork, but it doesn't yet know about the official or ["upstream" Timeplus repository](https://github.com/timeplus-io/timeplus-native-jdbc). Run the following commands:

    git remote add upstream https://github.com/timeplus-io/timeplus-native-jdbc.git
    git fetch upstream
    git branch --set-upstream-to=upstream/master master

Now, when you check the status using Git, it will compare your local repository to the *upstream* repository.

### Get the latest upstream code

You will frequently need to get all the of the changes that are made to the upstream repository, and you can do this with these commands:

    git fetch upstream
    git pull upstream master

The first command fetches all changes on all branches, while the second actually updates your local `master` branch with the latest commits from the `upstream` repository.

### Building locally

To build the source code locally, checkout and update the `master` branch:

    git checkout master
    git pull upstream master

Then use Maven to compile everything, build all artifacts, and install all JAR, ZIP, and TAR files into your local Maven repository:

    mvn clean install -DskipITs -DskipTests

(Currently the unit tests and integration tests are not fixed, so we have to skip them)

### Running and debugging tests
(Not working at this point)

A number of the modules use Docker during their integration tests to run a database. We use [TestContainers](https://www.testcontainers.org/) to manage that stuffs.
Please ensure that you have a docker daemon available when run integration tests on local

### Code Formatting

This project utilizes a set of code style rules that are automatically applied by the build process.

With the command `mvn validate` the code style rules can be applied automatically.

In the event that a pull request is submitted with code style violations, continuous integration will fail the pull request build.  

To fix pull requests with code style violations, simply run the project's build locally and allow the automatic formatting happen.  Once the build completes, you will have some local repository files modified to fix the coding style which can be amended on your pull request.  Once the pull request is synchronized with the formatting changes, CI will rerun the build.

To run the build, navigate to the project's root directory and run:

    mvn clean verify -DskipITs -DskipTests

It might be useful to simply run a _validate_ check against the code instead of automatically applying code style changes.  If you want to simply run validation, navigate to the project's root directory and run:

    mvn clean install -Dformat.formatter.goal=validate -Dformat.imports.goal=check

Please note that when running _validate_ checks, the build will stop as soon as it encounters its first violation.  This means it is necessary to run the build multiple times until no violations are detected.

### Documentation

When adding new features or configuration options, they must be documented accordingly in the Documents.
The same applies when changing existing behaviors, e.g. type mappings, removing options etc.

The documentation is written using Markdown and can be found in the timeplus-native-jdbc [source code repository](https://github.com/timeplus-io/timeplus-native-jdbc/docs).
Any documentation update should be part of the pull request you submit for the code change.

The documentation will be published on the website when PR merged into master branch.

### Publish new version to Maven
Change the revision number in pom.xml and timeplus-native-jdbc/pom.xml

Make sure you have proper codesign tool, then run the following commmands:

```bash
mvn clean  javadoc:jar source:jar package gpg:sign install deploy -DskipITs -DskipTests -e
```

This will geneate target/central-staging folder with required jar/asc/md5/sha1 files.

In some cases,the `asc` file is not properly generated and cannot be verified via `gpg --verify target/central-staging/com/timeplus/timeplus-native-jdbc/2.0.0/timeplus-native-jdbc-2.0.0.jar.asc`
You may need to regenerate it via `gpg -ab target/central-staging/com/timeplus/timeplus-native-jdbc/2.0.0/timeplus-native-jdbc-2.0.0.jar`

Then you turn the com folder to a zip file and create a deployment on https://central.sonatype.com/publishing/deployments with proper name and descript as well as the zip file.

Only jove@timeplus.com is expected to upload new version.


### Summary

Here's a quick check list for a good pull request (PR):

* A GitHub issue associated with your PR
* One commit per PR
* One feature/change per PR
* No changes to code not directly related to your change (e.g. no formatting changes or refactoring to existing code, if you want to refactor/improve existing code that's a separate discussion and separate GitHub issue)
* New/changed features have been documented
* A full build completes successfully
* Do a rebase on upstream `master`