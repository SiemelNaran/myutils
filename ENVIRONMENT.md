Environment notes
-----------------

Mac: Install jenv: `brew install jenv`

Install latest version of Java
Mac: `brew install --cask temurin`
Linux: `sudo apt install openjdk-24-jdk`

Add this version to jenv
Mac: `jenv add /Library/Java/JavaVirtualMachines/temurin-24.jdk/Contents/Home`

Before running mvn, set JAVA_HOME:
Mac: ``export JAVA_HOME=`jenv javahome```
