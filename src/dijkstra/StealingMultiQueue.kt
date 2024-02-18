package dijkstra

import kotlinx.atomicfu.atomic
import kotlin.random.Random
import java.util.*

class StealingMultiQueue<E>(numQueues: Int, private val comparator: Comparator<in E>): Queue<E> {
    private val nextFreeSlotId = atomic(0)

    // each thread writes to his own queue - no lock/guard required
    private val queues = Array(numQueues){ QueueWithStealingBuffer(comparator) }
    private val stolenTasks = Array(numQueues) { mutableListOf<E>() }

    private val mySlotIdx = ThreadLocal.withInitial { nextFreeSlotId.getAndIncrement() }

    override fun insert(x: E) {
        queues[mySlotIdx.get()].addLocal(x)
    }

    override fun delete(): E? {
        mySlotIdx.get().let { slot ->
            val myStolenTasks = stolenTasks[slot]
            if (myStolenTasks.isNotEmpty()) {
                return myStolenTasks.removeFirst()
            }
            if (Random.nextBoolean()) {
                val task = trySteal()
                if (task != null) return task
            }
            return queues[slot].extractTopLocal() ?: trySteal()
        }
    }

    private fun trySteal(): E? {
        mySlotIdx.get().let { slot ->
            val myQueue = queues[slot]
            val otherQueue = queues[Random.nextInt(queues.size)]
            if (myQueue == otherQueue) return null
            val myTop = myQueue.top()
            val otherTop = otherQueue.top()
            if (myTop == null && otherTop == null) return null
            if (myTop == null || comparator.compare(otherTop, myTop) < 0) {
                val tasks = otherQueue.steal()
                if (tasks.isEmpty()) return null
                val myStolenTasks = stolenTasks[slot]
                for (i in 1 until tasks.size) {
                    myStolenTasks.add(tasks[i])
                }
                return tasks[0]
            }
            return null
        }
    }
}

const val STEAL_SIZE = 3
private data class StealStatus(val epoch: Int, val stolen: Boolean)
private class QueueWithStealingBuffer<E>(comparator: Comparator<in E>) {
    private val q = PriorityQueue(comparator)
    private var stealingBuffer: MutableList<E> = mutableListOf()
    private val status = atomic(StealStatus(0, true))

    fun addLocal(x: E) {
        q.add(x)
        if (status.value.stolen) fillBuffer()
    }

    fun extractTopLocal(): E? {
        if (status.value.stolen) fillBuffer()
        return q.poll()
    }

    fun top(): E? {
        while (true) {
            val (epoch, stolen) = status.value
            if (stolen || stealingBuffer.isEmpty()) return null
            val top = stealingBuffer[0]
            if (status.value.epoch != epoch) continue
            return top
        }
    }

    fun steal(): List<E> {
        while (true) {
            val currStatus = status.value
            if (currStatus.stolen) return emptyList()
            val tasks = stealingBuffer
            if (!status.compareAndSet(currStatus, StealStatus(currStatus.epoch, true))) {
                continue
            }
            return tasks
        }
    }

    fun fillBuffer() {
        stealingBuffer = mutableListOf()
        for (i in 0 until STEAL_SIZE) {
            val task = q.poll() ?: break
            stealingBuffer.add(task)
        }
        status.value = StealStatus(status.value.epoch + 1, false)
    }
}
