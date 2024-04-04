from syndiffix import Synthesizer
from syndiffix.clustering.common import Clusters, ColumnId, StitchOwner


class ClusterInfo:
    """Extracts information about the clusters in a synthetic dataset."""

    STITCH_OWNER = 0
    STITCH_COLUMNS = 1
    DERIVED_COLUMNS = 2

    def __init__(self, sdx: Synthesizer) -> None:
        self.sdx = sdx

    def get_cluster_info(self) -> list:
        """Returns a list of clusters. Each cluster is a dict with:
        'cluster_id': the column indices in the cluster
        'cluster_name': the column names in the cluster
        'stitch_id': the column indices of the stitch columns
        'stitch_name': the column names of the stitch columns
        'type': the cluster type (initial, LEFT|RIGHT|SHARED stitch, patch)
        """
        cluster_id = self.sdx.clusters.initial_cluster
        cluster_name = [self.sdx.forest.columns[i] for i in cluster_id]
        cluster_info = [
            {"cluster_id": cluster_id, "cluster_name": cluster_name, "type": "initial"}
        ]
        for cluster in self.sdx.clusters.derived_clusters:
            if len(cluster[self.STITCH_COLUMNS]) == 0:
                cluster_type = "patch"
            else:
                cluster_type = cluster[self.STITCH_OWNER].name + "_stitch"
            cluster_type = cluster_type.lower()
            cluster_id = cluster[self.STITCH_COLUMNS] + cluster[self.DERIVED_COLUMNS]
            cluster_name = [self.sdx.forest.columns[i] for i in cluster_id]
            stitch_name = [
                self.sdx.forest.columns[i] for i in cluster[self.STITCH_COLUMNS]
            ]
            cluster_info.append(
                {
                    "cluster_name": cluster_name,
                    "stitch_name": stitch_name,
                    "cluster_id": cluster_id,
                    "stitch_id": cluster[self.STITCH_COLUMNS],
                    "type": cluster_type,
                }
            )
        return cluster_info
