package dijkstra

import kotlinx.atomicfu.atomicArrayOfNulls
import kotlinx.atomicfu.atomic
import java.util.*
import kotlin.random.Random

class ConcurrentMultiPriorityQueue<E>(numQueues: Int, private val comparator: Comparator<in E>): Queue<E> {
    private val queues = atomicArrayOfNulls<PriorityQueueWithLock<E>>(numQueues)

    override fun delete(): E? {
        while (true) {
            getDeleteQueue().apply {
                if (tryLock()) {
                    val result = poll()
                    unlock()
                    return result
                }
            }
        }
    }

    override fun insert(x: E) {
        while (true) {
            getInsertQueue().apply {
                if (tryLock()) {
                    add(x)
                    unlock()
                    return
                }
            }
        }
    }

    private fun getRandom(): Int {
        return Random.nextInt(queues.size)
    }

    private fun getInsertQueue(): PriorityQueueWithLock<E> {
        return getQueue(getRandom())
    }

    private fun getDeleteQueue(): PriorityQueueWithLock<E> {
        val a = getRandom()
        val qA = getQueue(a)
        if (queues.size == 1) return qA

        var b = a
        while (a == b) b = getRandom()
        val qB = getQueue(b)

        val qATop = qA.peek()
        val qBTop = qB.peek()
        return if (comparator.compare(qATop, qBTop) <= 0) qA else qB
    }

    private fun getQueue(idx: Int): PriorityQueueWithLock<E> {
        while (true) {
            val q = queues[idx].value
            if (q == null) {
                queues[idx].compareAndSet(null, PriorityQueueWithLock(comparator))
            } else return q
        }
    }
}

private class PriorityQueueWithLock<E>(comparator: Comparator<in E>): PriorityQueue<E>(comparator) {
    private val spinLock = atomic(false)
    fun tryLock() = spinLock.compareAndSet(expect = false, update = true)
    fun unlock() = spinLock.getAndSet(false)
}
