myutils
=======

This project is a collection of classes and scripts that I've found useful during my long experience with Java.
There are no tricky algorithms, unless I've found them useful in the practical world.

This project contains:
- Utilities for Java.
- A publish-subscribe framework in Java.
- An expression parser framework in Java.
- Miscellaneous shell script utilities.

See [OVERVIEW.md](OVERVIEW.md) for more details.


License
-------
This code is under the [Apache Licence v2](https://www.apache.org/licenses/LICENSE-2.0).


Building
--------

Install the following:
- Maven 3.6.3
- Java 16

To build all projects `mvn clean install`.
To build all projects skipping tests `mvn clean install -DskipTests`.


Maven
-----

To delete the files in the local repository
```
rm -rfv ~/.m2/repository/org/sn/myutils/
```


Eclipse IDE
-----------

Eclipse 2021-06 (4.20).

Steps:
- Optional: Ensure that .project and .classpath files are deleleted
```
find ../myutils/ -name "*.classpath" -exec rm -rfv {} \;
find ../myutils/ -name "*.project" -exec rm -rfv {} \;
```
- File -> Import -> Maven -> Existing Maven Projects
- Pick the myutils folder
- Ensure all checkboxes are checked and click Finish


IntelliJ IDE
------------

IntelliJ 2021.2.

Steps:
- Optional: Ensure that all .iml and the .idea folder are deleted.
  If you get incomprehensible compile errors about java.util classes not being found, invalidate cache and restart. Sometimes the following is needed:
```
find ../myutils/ -name "*.iml" -exec rm -rfv {} \;
rm -rfv ../myutils/.idea/
```
- Open or Import
- Pick the myutils folder and click OK

### Custom NotNull

Go to IntelliJ IDEA -> Preferences -> Editor -> Inspections -> @NotNull/@Nullable problems -> Configure Annotations<br/>
Add `org.sn.myutils.annotations.Nullable` to Nullable annotations<br/>
Add `org.sn.myutils.annotations.NotNull` to NotNull annotations
