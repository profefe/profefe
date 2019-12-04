This directory contains build files and documentation about how to create a
docker image for `profefe`.

Starting from the root of the project you can run:

```
make container
```

It will build an image called `profefe/profefe:<GITSHA>` where `<GITSHA>` is the
current git commit sha.

You can run the container with the command:

```
docker run -it -p 10100:10100 -v $PWD/data:$PWD/data profefe/profefe:<GITSHA>
```

If you need to change the http server port you can do:

```
docker run -it -p 10200:10200 -v $PWD/data:$PWD/data profefe/profefe:2550865 -addr :10200
```
