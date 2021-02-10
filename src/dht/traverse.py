import asyncio
import heapq
from collections import Counter
from typing import Dict, Awaitable, Callable, Any, Tuple, List, Set, Collection, Optional
from src.dht.routing import DHTID

ROOT = 0


async def simple_traverse_dht(query_id: DHTID, initial_nodes: Collection[DHTID], beam_size: int,
                              get_neighbors: Callable[[DHTID], Awaitable[Tuple[Collection[DHTID], bool]]],
                              visited_nodes: Collection[DHTID] = ()) -> Tuple[Tuple[DHTID], Set[DHTID]]:
    visited_nodes = set(visited_nodes)
    initial_nodes = [node_id for node_id in initial_nodes if node_id not in visited_nodes]
    if not initial_nodes:
        return (), visited_nodes
    unvisited_nodes = [(distance, uid) for uid, distance in zip(initial_nodes, query_id.xor_distance(initial_nodes))]
    heapq.heapify(unvisited_nodes)
    nearest_nodes = [(-distance, node_id) for distance, node_id in heapq.nsmallest(beam_size, unvisited_nodes)]
    heapq.heapify(nearest_nodes)
    while len(nearest_nodes) > beam_size:
        heapq.heappop(nearest_nodes)
    visited_nodes |= set(initial_nodes)
    upper_bound = -nearest_nodes[0][0]
    was_interrupted = False
    while (not was_interrupted) and len(unvisited_nodes) != 0 and unvisited_nodes[0][0] <= upper_bound:
        _, node_id = heapq.heappop(unvisited_nodes)
        neighbors, was_interrupted = await get_neighbors(node_id)
        neighbors = [node_id for node_id in neighbors if node_id not in visited_nodes]
        visited_nodes.update(neighbors)
        for neighbor_id, distance in zip(neighbors, query_id.xor_distance(neighbors)):
            if distance <= upper_bound or len(nearest_nodes) < beam_size:
                heapq.heappush(unvisited_nodes, (distance, neighbor_id))
                heapq_add_or_replace = heapq.heappush if len(nearest_nodes) < beam_size else heapq.heappushpop
                heapq_add_or_replace(nearest_nodes, (-distance, neighbor_id))
                upper_bound = -nearest_nodes[0][0]
    return tuple(node_id for _, node_id in heapq.nlargest(beam_size, nearest_nodes)), visited_nodes


async def traverse_dht(queries: Collection[DHTID], initial_nodes: List[DHTID], beam_size: int, num_workers: int,
                       queries_per_call: int, get_neighbors: Callable[
            [DHTID, Collection[DHTID]], Awaitable[Dict[DHTID, Tuple[Tuple[DHTID], bool]]]],
                       found_callback: Optional[Callable[[DHTID, List[DHTID], Set[DHTID]], Awaitable[Any]]] = None,
                       await_all_tasks: bool = True, visited_nodes: Optional[Dict[DHTID, Set[DHTID]]] = (), ) -> Tuple[
    Dict[DHTID, List[DHTID]], Dict[DHTID, Set[DHTID]]]:
    if len(queries) == 0:
        return {}, dict(visited_nodes or {})
    unfinished_queries = set(queries)
    candidate_nodes: Dict[DHTID, List[Tuple[int, DHTID]]] = {}
    nearest_nodes: Dict[DHTID, List[Tuple[int, DHTID]]] = {}
    known_nodes: Dict[DHTID, Set[DHTID]] = {}
    visited_nodes: Dict[DHTID, Set[DHTID]] = dict(visited_nodes or {})
    pending_tasks = set()
    active_workers = Counter({q: 0 for q in queries})
    search_finished_event = asyncio.Event()
    heap_updated_event = asyncio.Event()
    heap_updated_event.set()
    for query in queries:
        distances = query.xor_distance(initial_nodes)
        candidate_nodes[query] = list(zip(distances, initial_nodes))
        nearest_nodes[query] = list(zip([-d for d in distances], initial_nodes))
        heapq.heapify(candidate_nodes[query])
        heapq.heapify(nearest_nodes[query])
        while len(nearest_nodes[query]) > beam_size:
            heapq.heappop(nearest_nodes[query])
        known_nodes[query] = set(initial_nodes)
        visited_nodes[query] = set(visited_nodes.get(query, ()))

    def heuristic_priority(heap_query: DHTID):
        if has_candidates(heap_query):
            return active_workers[heap_query], candidate_nodes[heap_query][ROOT][0]
        return float('inf'), float('inf')

    def has_candidates(query: DHTID):
        return candidate_nodes[query] and candidate_nodes[query][ROOT][0] <= upper_bound(query)

    def upper_bound(query: DHTID):
        return -nearest_nodes[query][ROOT][0] if len(nearest_nodes[query]) >= beam_size else float('inf')

    def finish_search(query):
        unfinished_queries.remove(query)
        if len(unfinished_queries) == 0:
            search_finished_event.set()
        if found_callback:
            nearest_neighbors = [peer for _, peer in heapq.nlargest(beam_size, nearest_nodes[query])]
            pending_tasks.add(asyncio.create_task(found_callback(query, nearest_neighbors, set(visited_nodes[query]))))

    async def worker():
        while unfinished_queries:
            chosen_query: DHTID = min(unfinished_queries, key=heuristic_priority)
            if not has_candidates(chosen_query):
                other_workers_pending = active_workers.most_common(1)[0][1] > 0
                if other_workers_pending:
                    heap_updated_event.clear()
                    await heap_updated_event.wait()
                    continue
                else:
                    for query in list(unfinished_queries):
                        finish_search(query)
                    break
            chosen_distance_to_query, chosen_peer = heapq.heappop(candidate_nodes[chosen_query])
            if chosen_peer in visited_nodes[chosen_query] or chosen_distance_to_query > upper_bound(chosen_query):
                if chosen_distance_to_query > upper_bound(chosen_query) and active_workers[chosen_query] == 0:
                    finish_search(chosen_query)
                continue
            possible_additional_queries = [query for query in unfinished_queries if
                                           query != chosen_query and chosen_peer not in visited_nodes[query]]
            queries_to_call = [chosen_query] + heapq.nsmallest(queries_per_call - 1, possible_additional_queries,
                                                               key=chosen_peer.xor_distance)
            active_workers.update(queries_to_call)
            for query_to_call in queries_to_call:
                visited_nodes[query_to_call].add(chosen_peer)
            get_neighbors_task = asyncio.create_task(get_neighbors(chosen_peer, queries_to_call))
            pending_tasks.add(get_neighbors_task)
            await asyncio.wait([get_neighbors_task, search_finished_event.wait()], return_when=asyncio.FIRST_COMPLETED)
            if search_finished_event.is_set():
                break
            pending_tasks.remove(get_neighbors_task)
            for query, (neighbors_for_query, should_stop) in get_neighbors_task.result().items():
                if should_stop and (query in unfinished_queries):
                    finish_search(query)
                if query not in unfinished_queries:
                    continue
                for neighbor in neighbors_for_query:
                    if neighbor not in known_nodes[query]:
                        known_nodes[query].add(neighbor)
                        distance = query.xor_distance(neighbor)
                        if distance <= upper_bound(query) or len(nearest_nodes[query]) < beam_size:
                            heapq.heappush(candidate_nodes[query], (distance, neighbor))
                            if len(nearest_nodes[query]) < beam_size:
                                heapq.heappush(nearest_nodes[query], (-distance, neighbor))
                            else:
                                heapq.heappushpop(nearest_nodes[query], (-distance, neighbor))
            active_workers.subtract(queries_to_call)
            heap_updated_event.set()

    workers = [asyncio.create_task(worker()) for _ in range(num_workers)]
    try:
        await asyncio.wait(workers, return_when=asyncio.FIRST_COMPLETED)
        assert len(unfinished_queries) == 0 and search_finished_event.is_set()
        if await_all_tasks:
            await asyncio.gather(*pending_tasks)
        nearest_neighbors_per_query = {query: [peer for _, peer in heapq.nlargest(beam_size, nearest_nodes[query])] for
                                       query in queries}
        return nearest_neighbors_per_query, visited_nodes
    except asyncio.CancelledError as e:
        for worker in workers:
            worker.cancel()
        raise e
