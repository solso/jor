
## (J)SON (o)ver (R)edis

__This project is still WIP__

Using redis as storage for JSON documents. JSON are stored as BLOBS in redis, however indexes are build for all
JSON fields to have fast find (retrieval) by any field of the document. 

The API is intentionally similar to that of MongoDB (a strip down version of it).

This gems focuses on fast create and read operations over the collection of JSON documents. Updates are only possible at the level of the whole document.


