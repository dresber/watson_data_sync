"""
"""

# --------------------------------------- #
#               imports                   #
# --------------------------------------- #
from elasticsearch import Elasticsearch

from utils.logging_handle import LoggingHandle

# --------------------------------------- #
#              definitions                #
# --------------------------------------- #
MODULE_LOGGER_HEAD = "es_handle -> "

# --------------------------------------- #
#              global vars                #
# --------------------------------------- #
logger = LoggingHandle()

# --------------------------------------- #
#              functions                  #
# --------------------------------------- #


# --------------------------------------- #
#               classes                   #
# --------------------------------------- #
class ESHandle(object):
    """ does the connection and handling with ES server
    """
    def __init__(self, host, port):
        try:
            self.es = Elasticsearch([{"host": host, "port": port}])
        except Exception as e:
            logger.error(MODULE_LOGGER_HEAD + "could not connect ES: {}".format(e))

    def create_index(self, index_name):
        try:
            self.es.indices.create(index=index_name, ignore=400)
        except Exception as e:
            logger.error(MODULE_LOGGER_HEAD + "could not create index {} reason: {}".format(index_name, e))

    def upload_doc(self, index_name, doc_type, doc_dict):
        try:
            self.es.index(index=index_name, doc_type=doc_type, body=doc_dict)
        except Exception as e:
            logger.error(MODULE_LOGGER_HEAD + "could not upload doc reason: {}".format(e))

    def get_doc(self, index_name, doc_type, query=None):
        try:
            return self.es.search(index=index_name, doc_type=doc_type, body=query)
        except Exception as e:
            logger.error(MODULE_LOGGER_HEAD + "could not get document from ES reason: {}".format(e))

    def delete_entry(self, index_name, doc_type, id):
        try:
            self.es.delete(index=index_name, doc_type=doc_type, id=id)
        except Exception as e:
            logger.error(MODULE_LOGGER_HEAD + "could ot delete id {} reason: {}".format(id, e))

    def update_doc(self, index_name, doc_type, id, doc_dict):
        try:
            self.es.update(index=index_name, doc_type=doc_type, id=id, body={"doc": doc_dict})
        except Exception as e:
            logger.error(MODULE_LOGGER_HEAD + "could not update entry {} reason: {}".format(id, e))


# --------------------------------------- #
#                main                     #
# --------------------------------------- #
if __name__ == "__main__":
    pass

