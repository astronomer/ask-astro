from invoke import Collection

from tasks import api, docs

ns = Collection()
ns.add_collection(api)
ns.add_collection(docs)
