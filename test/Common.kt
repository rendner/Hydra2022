package hydra2022

const val THREADS = 3
const val ACTORS_PER_THREAD = 3

class IntQueueSequential {
    private val q = ArrayDeque<Int>()

    fun enqueue(x: Int) {
        q.addLast(x)
    }

    fun dequeue(): Int? = q.removeFirstOrNull()
}