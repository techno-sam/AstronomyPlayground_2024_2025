import pandas as pd


class Cluster:
    def __init__(self, name: str, dm: float, log_age: float, fe_h: float, e_b_sub_v: float, memb: int, open_cluster: bool, color: int):
        self.name = name
        self.dm = dm
        self.log_age = log_age
        self.fe_h = fe_h
        self.e_b_sub_v = e_b_sub_v
        self.memb = memb
        self.open_cluster = open_cluster
        self.color = color
        self._data: pd.DataFrame | None = None

    @property
    def distance_pc(self) -> float:
        return 10 ** ((self.dm + 5) / 5)

    @property
    def cluster_type(self) -> str:
        if self.open_cluster is None:
            return "Unknown"
        return "Open" if self.open_cluster else "Globular"

    @property
    def data(self) -> pd.DataFrame:
        if self._data is None:
            self._data = self._load_data()
        return self._data

    def _load_data(self) -> pd.DataFrame:
        return pd.read_csv(f"cache/{self.name}.csv")

    def get_info_label(self, indent: str = "") -> str:
        return f"{indent}Cluster Type: {self.cluster_type}\n{indent}log(age): {self.log_age}\n{indent}Distance: {self.distance_pc:.0f} pc"

    def __str__(self):
        return f"{self.name}"

    def __repr__(self):
        return f"{self.name}(dm={self.dm}, log_age={self.log_age}, fe_h={self.fe_h}, e_b_sub_v={self.e_b_sub_v}, memb={self.memb}, dist_ly={self.dist_ly})"


clusters = [
    Cluster("IC 2391", 5.908, 7.70, -0.01, 0.030, 254, True, 0x648FFF),
    Cluster("NGC 6475", 7.234, 8.54, 0.02, 0.049, 874, True, 0x785EF0),
    Cluster("NGC 2360", 10.229, 8.98, -0.03, 0.090, 848, True, 0xDC267F),
    Cluster("NGC 6793", 8.894, 8.78, float("nan"), 0.272, 271, True, 0xFE6100),
    Cluster("NGC 2232", 7.575, 7.70, 0.11, 0.031, 241, True, 0xFFB000)
]