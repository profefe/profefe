This directory contains build files and documentation about how to create a
docker image for `profefe`.

Starting from the root of the project you can run:

```
make docker-image
```

This will build an image named `profefe/profefe:git-<GITSHA>` where `<GITSHA>` is the
current git commit.

You can run the container with the command:

```
docker run -it -p 10100:10100 -v $PWD/data:/data profefe/profefe:git-<GITSHA>
```

If you need to change the http server port and you can do:

```
docker run -it -p 10200:10200 -v $PWD/data:/data profefe/profefe:git-<GITSHA> -addr :10200
```
