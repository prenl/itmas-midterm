from __future__ import annotations

import torch
import torch.nn as nn

try:
    from torch_geometric.nn import GATConv, HeteroConv, Linear
except ImportError:  # pragma: no cover
    GATConv = None
    HeteroConv = None
    Linear = None


class TradeGNNRecommender(nn.Module):
    """Heterogeneous GAT for trade recommendation."""

    def __init__(self, hidden_channels: int = 32, heads: int = 2):
        super().__init__()
        if GATConv is None or HeteroConv is None or Linear is None:
            raise ImportError("Install torch-geometric to use the recommender model.")

        self.country_lin = Linear(-1, hidden_channels)
        self.hs6_lin = Linear(-1, hidden_channels)

        self.conv1 = HeteroConv(
            {
                ("country", "exports_to", "country"): GATConv(
                    (-1, -1), hidden_channels, heads=heads, add_self_loops=False
                ),
                ("country", "trades_hs6", "hs6"): GATConv(
                    (-1, -1), hidden_channels, heads=heads, add_self_loops=False
                ),
            },
            aggr="sum",
        )

        self.conv2 = HeteroConv(
            {
                ("country", "exports_to", "country"): GATConv(
                    (hidden_channels * heads, hidden_channels * heads),
                    hidden_channels,
                    heads=1,
                    add_self_loops=False,
                ),
                ("country", "trades_hs6", "hs6"): GATConv(
                    (hidden_channels * heads, hidden_channels * heads),
                    hidden_channels,
                    heads=1,
                    add_self_loops=False,
                ),
            },
            aggr="sum",
        )

        self.score_head = nn.Sequential(
            nn.Linear(hidden_channels * 2, hidden_channels),
            nn.ReLU(),
            nn.Linear(hidden_channels, 1),
        )

    def encode(self, data):
        # Project node features.
        x_dict = {
            "country": self.country_lin(data["country"].x),
            "hs6": self.hs6_lin(data["hs6"].x),
        }

        # Message passing with attention.
        x_dict = self.conv1(x_dict, data.edge_index_dict)
        x_dict = {k: v.relu() for k, v in x_dict.items()}
        x_dict = self.conv2(x_dict, data.edge_index_dict)
        return x_dict

    def score_country_country(self, source_country_emb: torch.Tensor, target_country_emb: torch.Tensor):
        joined = torch.cat([source_country_emb, target_country_emb], dim=-1)
        return self.score_head(joined)

    def score_country_hs6(self, country_emb: torch.Tensor, hs6_emb: torch.Tensor):
        joined = torch.cat([country_emb, hs6_emb], dim=-1)
        return self.score_head(joined)


def training_step(model, data, source_country_idx: int, target_country_idx: int, label: float):
    x_dict = model.encode(data)
    source_country_emb = x_dict["country"][source_country_idx]
    target_country_emb = x_dict["country"][target_country_idx]
    pred = model.score_country_country(source_country_emb, target_country_emb)
    target = torch.tensor([label], dtype=torch.float, device=pred.device)
    loss = torch.nn.functional.binary_cross_entropy_with_logits(pred.view(-1), target)
    return loss
