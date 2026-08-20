"""
ACIS-X Model Governance Package.

Provides autonomous model risk management (MRM) agents for:
- Statistical concept and feature drift tracking (PSI, KS-test)
- Challenger model training and benchmarking
- Audit report generation for regulatory model risk governance
"""

from agents.governance.model_governance_agent import ModelGovernanceAgent

__all__ = ["ModelGovernanceAgent"]
