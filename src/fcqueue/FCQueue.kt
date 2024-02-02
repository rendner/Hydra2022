package fcqueue

import kotlinx.atomicfu.atomic
import kotlinx.atomicfu.atomicArrayOfNulls

class FCQueue<E>(numThreadSlots: Int) {
    private val nextFreeSlotId = atomic(0)
    private val pendingOperations = atomicArrayOfNulls<Operation<E>>(numThreadSlots)

    private val myOperation = ThreadLocal.withInitial {
        Operation<E>().also {
            pendingOperations[nextFreeSlotId.getAndIncrement()].value = it
        }
    }

    private val spinLock = atomic(false)
    private val q = ArrayDeque<E>()

    private fun tryLock() = spinLock.compareAndSet(expect = false, update = true)
    private fun unlock() = spinLock.getAndSet(false)

    private fun scanCombineApply() {
        for (i in 0 until pendingOperations.size) {
            pendingOperations[i].value?.let { op ->
                when (op.getStatus()) {
                    OperationStatus.PENDING_ENQUEUE -> {
                        q.addLast(op.getValue())
                        op.markAsDone()
                    }
                    OperationStatus.PENDING_DEQUEUE -> {
                        op.markAsDone(q.removeFirstOrNull())
                    }
                    else -> Unit
                }
            }
        }
    }

    /**
     * Adds the specified element [x] to the queue.
     */
    fun enqueue(x: E) {
        myOperation.get().apply {
            requestEnqueue(x)

            while (true) {
                if (tryLock()) {
                    scanCombineApply()
                    unlock()
                    return
                } else if (isDone()) return
            }
        }
    }

    /**
     * Retrieves the first element from the queue
     * and returns it; returns `null` if the queue
     * is empty.
     */
    fun dequeue(): E? {
        myOperation.get().apply {
            requestDequeue()

            while (true) {
                if (tryLock()) {
                    scanCombineApply()
                    unlock()
                    break
                } else if (isDone()) break
            }

            return getResult()
        }
    }
}

private enum class OperationStatus {
    PENDING_ENQUEUE,
    PENDING_DEQUEUE,
    DONE
}
private class Operation<E> {
    private var result: E? = null
    private var value: E? = null
    // write status always as last action to also publish modified result and value
    // https://stackoverflow.com/questions/4614331/using-volatile-keyword-with-mutable-object
    private val status = atomic(OperationStatus.DONE)

    fun requestEnqueue(value: E) {
        result = null
        this.value = value
        status.value = OperationStatus.PENDING_ENQUEUE
    }

    fun requestDequeue() {
        result = null
        value = null
        status.value = OperationStatus.PENDING_DEQUEUE
    }

    fun markAsDone(result: E? = null) {
        this.result = result
        value = null
        status.value = OperationStatus.DONE
    }

    fun isDone() = status.value == OperationStatus.DONE
    fun getResult() = result
    fun getStatus() = status.value
    fun getValue() = value as E
}
