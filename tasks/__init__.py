from invoke import Collection

from tasks import api, docs, ui

ns = Collection()
ns.add_collection(api)
ns.add_collection(docs)
ns.add_collection(ui)
