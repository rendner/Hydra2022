package dijkstra

import kotlinx.atomicfu.atomic
import java.util.*
import java.util.concurrent.Phaser
import java.util.concurrent.atomic.AtomicInteger
import kotlin.concurrent.thread

private val USE_STEALING_MULTI_QUEUE = false

fun shortestPathParallel(start: Node) {
    val workers = Runtime.getRuntime().availableProcessors()
    // The distance to the start node is `0`
    start.distance = 0

    // Create a priority (by distance) queue and add the start node into it
    val q = if (USE_STEALING_MULTI_QUEUE)
        StealingMultiQueue(workers + 1, NODE_DISTANCE_COMPARATOR)
    else
        ConcurrentMultiPriorityQueue(workers * 2, NODE_DISTANCE_COMPARATOR)

    q.insert(start)
    val activeNodes = AtomicInteger(1)
    // Run worker threads and wait until the total work is done
    val onFinish = Phaser(workers + 1) // `arrive()` should be invoked at the end by each worker
    repeat(workers) {
        thread {
            while (activeNodes.get() > 0) {
                val cur: Node = q.delete() ?: continue

                for (e in cur.outgoingEdges) {
                    val distance = cur.distance + e.weight
                    if (e.to.updateDistIfLower(distance)) {
                        activeNodes.incrementAndGet()
                        q.insert(e.to)
                    }
                }

                activeNodes.decrementAndGet()
            }
            onFinish.arrive()
        }
    }
    onFinish.arriveAndAwaitAdvance()
}

interface Queue<E> {
    fun insert(x: E)
    fun delete(): E?
}

class Node {
    private val _outgoingEdges = arrayListOf<Edge>()
    val outgoingEdges: List<Edge> = _outgoingEdges

    private val _distance = atomic(Integer.MAX_VALUE)
    var distance
        get() = _distance.value
        set(value) {
            _distance.value = value
        }

    fun updateDistIfLower(update: Int): Boolean {
        while (true) {
            val d = _distance.value
            if (update < d) {
                if (_distance.compareAndSet(d, update)) {
                    return true
                }
                continue
            }
            return false
        }
    }

    fun addEdge(edge: Edge) {
        _outgoingEdges.add(edge)
    }
}

val NODE_DISTANCE_COMPARATOR = Comparator<Node> { o1: Node?, o2: Node? ->
    if (o1 == null && o2 == null) 0
    else if (o2 == null) -1
    else if (o1 == null) 1
    else o1.distance.compareTo(o2.distance)
}

data class Edge(val to: Node, val weight: Int)
