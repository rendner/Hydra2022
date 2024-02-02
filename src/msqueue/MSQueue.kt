package msqueue

import kotlinx.atomicfu.AtomicRef
import kotlinx.atomicfu.atomic

class MSQueue<E> {
    private val head: AtomicRef<Node<E>>
    private val tail: AtomicRef<Node<E>>

    init {
        val dummy = Node<E>(null)
        head = atomic(dummy)
        tail = atomic(dummy)
    }

    /**
     * Adds the specified element [x] to the queue.
     */
    fun enqueue(x: E) {
        val newTail = Node(x)
        while (true) {
            val tail = this.tail.value
            if (tail.next.compareAndSet(null, newTail)) {
                this.tail.compareAndSet(tail, newTail)
                break
            } else {
                // required to guarantee Obstruction-freedom
                tail.next.value?.let { this.tail.compareAndSet(tail, it) }
            }
        }
    }

    /**
     * Retrieves the first element from the queue
     * and returns it; returns `null` if the queue
     * is empty.
     */
    fun dequeue(): E? {
        while (true) {
            val head = this.head.value
            val headNext = head.next.value ?: return null
            if (this.head.compareAndSet(head, headNext)) {
                return headNext.x
            }
        }
    }
}

private class Node<E>(val x: E?) {
    val next = atomic<Node<E>?>(null)
}
