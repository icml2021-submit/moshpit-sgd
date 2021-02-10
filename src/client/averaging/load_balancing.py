from typing import Sequence, Optional, Tuple
import numpy as np
import scipy.optimize
from src.utils.logging import get_logger

logger = get_logger(__name__)


def load_balance_peers(vector_size, throughputs: Sequence[Optional[float]], min_size: int = 0) -> Tuple[int, ...]:
    specified_throughputs = [throughput for throughput in throughputs if throughput is not None and throughput > 0]
    if specified_throughputs:
        default_throughput = np.mean(specified_throughputs)
        throughputs = [throughput if throughput is not None else default_throughput for throughput in throughputs]
        scores = optimize_parts_lp(vector_size, np.asarray(throughputs), min_size)
    else:
        assert not all(throughput == 0 for throughput in throughputs), "Must have at least one nonzero throughput"
        scores = np.asarray([1.0 if throughput is None else 0.0 for throughput in throughputs])
    return tuple(hagenbach_bishoff(vector_size, scores))


def optimize_parts_lp(vector_size: int, throughputs: np.ndarray, min_size: int = 0, eps: float = 1e-15) -> np.ndarray:
    assert np.all(throughputs >= 0) and np.any(throughputs > 0)
    permutation = np.argsort(-throughputs)
    throughputs = throughputs[permutation]
    is_nonzero = throughputs != 0
    group_size = len(throughputs)
    num_variables = group_size + 1
    c = np.zeros(num_variables)
    c[-1] = 1.0
    nonnegative_weights = -np.eye(group_size, M=num_variables), np.zeros(group_size)
    weights_sum_to_one = c[None, :] - 1.0, np.array([-1.0])
    coeff_per_variable = (group_size - 2.0) / np.maximum(throughputs, eps)
    coeff_matrix_minus_xi = np.hstack([np.diag(coeff_per_variable), -np.ones((group_size, 1))])
    xi_is_maximum = coeff_matrix_minus_xi[is_nonzero], -1.0 / throughputs[is_nonzero]
    force_max_weights = np.eye(group_size, M=num_variables), is_nonzero.astype(c.dtype)
    A, b = list(map(np.concatenate, zip(nonnegative_weights, weights_sum_to_one, xi_is_maximum, force_max_weights)))
    solution = scipy.optimize.linprog(c, A_ub=A, b_ub=b)
    if solution.success:
        peer_scores = solution.x[:group_size]
        if np.max(peer_scores) >= min_size / float(vector_size):
            peer_scores[peer_scores < min_size / float(vector_size)] = 0.0
    else:
        logger.error(f"Failed to solve load-balancing for bandwidths {throughputs}.")
        peer_scores = np.ones(group_size)
    return peer_scores[np.argsort(permutation)]


def hagenbach_bishoff(vector_size: int, scores: Sequence[float]) -> Sequence[int]:
    total_score = sum(scores)
    allocated = [int(vector_size * score_i / total_score) for score_i in scores]
    while sum(allocated) < vector_size:
        quotients = [score / (allocated[idx] + 1) for idx, score in enumerate(scores)]
        idx_max = quotients.index(max(quotients))
        allocated[idx_max] += 1
    return allocated
