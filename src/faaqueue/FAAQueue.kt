package faaqueue

import kotlinx.atomicfu.*

class FAAQueue<E> {
    private val head: AtomicRef<Segment> // Head pointer, similarly to the Michael-Scott queue (but the first node is _not_ sentinel)
    private val tail: AtomicRef<Segment> // Tail pointer, similarly to the Michael-Scott queue

    private val enqIdx = atomic(0L) // Global index for the next enqueue operation
    private val deqIdx = atomic(0L) // Global index for the next dequeue operation

    private val poison = -1

    init {
        val firstNode = Segment(0)
        head = atomic(firstNode)
        tail = atomic(firstNode)
    }

    private fun findSegment(start: Segment, id: Long): Segment {

        var curr = start

        while (curr.id < id) {
            val sId = curr.id + 1
            val newTail = Segment(sId)
            curr.next.compareAndSet(null, newTail)

            curr = curr.next.value!!
        }

        return curr
    }

    /**
     * Adds the specified element [x] to the queue.
     */
    fun enqueue(x: E) {
        while (true) {
            val tail = this.tail.value
            val enqIdx = this.enqIdx.getAndIncrement()

            val s = findSegment(tail, enqIdx / SEGMENT_SIZE)

            if (s.id > tail.id) {
                this.tail.compareAndSet(tail, s)
            }

            val i = (enqIdx % SEGMENT_SIZE).toInt()
            if (s.elements[i].compareAndSet(null, x)) return
        }
    }

    /**
     * Retrieves the first element from the queue
     * and returns it; returns `null` if the queue
     * is empty.
     */
    @Suppress("UNCHECKED_CAST")
    fun dequeue(): E? {
        while (true) {
            if (deqIdx.value >= enqIdx.value) return null
            val head = this.head.value
            val deqIdx = this.deqIdx.getAndIncrement()

            val s = findSegment(head, deqIdx / SEGMENT_SIZE)

            if (s.id > head.id) {
                this.head.compareAndSet(head, s)
            }

            val i = (deqIdx % SEGMENT_SIZE).toInt()
            if (s.elements[i].compareAndSet(null, poison)) continue
            return s.elements[i].value as E
        }
    }
}

private class Segment(val id: Long) {
    val next = atomic<Segment?>(null)
    val elements = atomicArrayOfNulls<Any>(SEGMENT_SIZE)
}

const val SEGMENT_SIZE = 2 // DO NOT CHANGE, IMPORTANT FOR TESTS

