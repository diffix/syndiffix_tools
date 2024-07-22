from typing import Any
from syndiffix import Synthesizer
from syndiffix.clustering.common import Clusters, ColumnId, StitchOwner


class ClusterInfo:
    """Extracts information about the clusters in a synthetic dataset."""

    STITCH_OWNER = 0
    STITCH_COLUMNS = 1
    DERIVED_COLUMNS = 2

    def __init__(self, sdx: Synthesizer) -> None:
        self.sdx = sdx

    def put_initial_cluster(self, cluster: list[str], final: bool = False) -> None:
        ''' The cluster is a list of column names.
            Set final to True is this is the only cluster.
        '''
        # make sure there are no duplicate values in cluster
        if len(cluster) != len(set(cluster)):
            raise ValueError("Duplicate column names in cluster.")
        if len(cluster) == 0:
            raise ValueError("Cluster must contain at least one column.")
        # Convert the list of names into a list of column indices
        self.sdx.clusters.initial_cluster = []
        for column_name in cluster:
            try:
                column_id = self.sdx.forest.columns.index(column_name)
            except:
                raise ValueError(f"Column {column_name} not found.")
            self.sdx.clusters.initial_cluster.append(ColumnId(column_id))
        self._check_clusters(final)

    def put_derived_cluster(self, owner: str = 'shared',
                            stitch_columns: list[str] = [],
                            derived_columns: list[str] = [],
                            final: bool = False) -> None:
        ''' The owner can be 'shared', 'left', or 'right'
            The stitch_columns list is empty of the derived_columns are patched.
            Set final to True is this is the last derived cluster.
        '''
        # make sure there are no duplicate values
        if len(stitch_columns) != len(set(stitch_columns)):
            raise ValueError("Duplicate column names in stitch_columns.")
        if len(derived_columns) != len(set(derived_columns)):
            raise ValueError("Duplicate column names in derived_columns.")
        if len(derived_columns) == 0:
            raise ValueError("derived_columns must contain at least one column.")
        derived_cluster = [None, [], []]
        if owner == 'shared':
            derived_cluster[self.STITCH_OWNER] = StitchOwner.SHARED
        elif owner == 'left':
            derived_cluster[self.STITCH_OWNER] = StitchOwner.LEFT
        elif owner == 'right':
            derived_cluster[self.STITCH_OWNER] = StitchOwner.RIGHT
        else:
            raise ValueError(f"Owner {owner} not recognized. Must be 'shared', 'left', or 'right'.")
        for column_name in stitch_columns:
            try:
                column_id = self.sdx.forest.columns.index(column_name)
            except:
                raise ValueError(f"Column {column_name} not found.")
            derived_cluster[self.STITCH_COLUMNS].append(ColumnId(column_id))
        for column_name in derived_columns:
            try:
                column_id = self.sdx.forest.columns.index(column_name)
            except:
                raise ValueError(f"Column {column_name} not found.")
            derived_cluster[self.DERIVED_COLUMNS].append(ColumnId(column_id))
        self.sdx.clusters.derived_clusters.append(derived_cluster)
        self._check_clusters(final)

    def _check_clusters(self, final: bool) -> bool:
        checker = [False] * len(self.sdx.forest.columns)
        for column_id in self.sdx.clusters.initial_cluster:
            if checker[column_id] is True:
                raise ValueError(f"Duplicate column detected (initial) {column_id}")
            checker[column_id] = True
        for cluster in self.sdx.clusters.derived_clusters:
            for column_id in cluster[self.STITCH_COLUMNS]:
                if checker[column_id] is False:
                    raise ValueError(f"Stitch column not in prior cluster {column_id}.")
            for column_id in cluster[self.DERIVED_COLUMNS]:
                if checker[column_id] is True:
                    raise ValueError(f"Duplicate column detected (derived) {column_id}")
                checker[column_id] = True
        if final:
            for i in range(len(checker)):
                if checker[i] is False:
                    raise ValueError(f"Column not in any cluster {i}")

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
