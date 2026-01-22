cp /Users/asifbashar/Documents/codebase/git/opensearch/opensource/forked/sql-override-files-doctest/bootstrap.sh doctest/
./gradlew :doctest:stopOpenSearch
DOCTEST_FLAGS="-f" ./gradlew :doctest:doctest --continue -PignoreFailures=true



