
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.future.await
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import java.util.*
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors
import kotlin.random.asKotlinRandom

private const val DYNAMO_DB_MINIMUM_LATENCY = 20L
private const val DYNAMO_DB_MAXIMUM_LATENCY = 50L

private val DYNAMO_DB_THREAD_POOL = Executors.newFixedThreadPool(50)

data class Transaction(val id: UUID, val account: Account)

data class Account(val id: UUID)

fun main() {

    // Get transactions from kafka
    val transactions = getTransactions()

    val start = System.currentTimeMillis()

    // Group transactions by account
    val transactionsByAccount: Map<Account, List<Transaction>> = transactions.groupBy { it.account }

    // Get Set of accounts
    val accounts: Set<Account> = transactionsByAccount.keys

    // Get Accounts that exists in a reactive fashion
    val accountsInDynamo: Flow<Account> = getAccountsFromDynamo(accounts)

    runBlocking {
        accountsInDynamo.collect { account ->
            val transactionsFromAccount: List<Transaction> = transactionsByAccount[account]!!

            transactionsFromAccount
                .map { Pair(it, existsOrPut(it)) }
                .forEach {
                    val (transaction, idempotencyFuture) = it

                    launch {
                        val exists = idempotencyFuture.await()
                        if (!exists) log.info("Lançamento ${transaction.id} produzido!")
                    }
                }

        }
    }

    val end = System.currentTimeMillis()
    println("Tempo de execução total: ${(end - start)} ms")
    DYNAMO_DB_THREAD_POOL.shutdown()
}

private val log = object {
    fun info(message: String) {
        val threadId = Thread.currentThread().id
        println("Thread $threadId | $message")
    }
}

private fun getTransactions(): List<Transaction> {
    val transactions = mutableListOf<Transaction>()

    for (n in 0 until 1000) {
        transactions.add(Transaction(
            id = UUID.randomUUID(),
            account = Account(UUID.randomUUID())
        ))
    }

    return transactions
}

@OptIn(ExperimentalCoroutinesApi::class)
private fun getAccountsFromDynamo(accountsId: Set<Account>): Flow<Account> = channelFlow {
    accountsId.chunked(100)
        .map { getBatchRead(it) }
        .forEach { completableFuture ->
            launch {
                val accounts: Set<Account> = completableFuture.await()
                log.info("Batch de contas carregado!")
                accounts.forEach { send(it) }
            }
        }
}


private fun getBatchRead(accounts: List<Account>): CompletableFuture<Set<Account>> {
    val completableFuture = CompletableFuture<Set<Account>>()

    DYNAMO_DB_THREAD_POOL.submit {
        val sleep = Random().asKotlinRandom().nextLong(DYNAMO_DB_MINIMUM_LATENCY, DYNAMO_DB_MAXIMUM_LATENCY)
        Thread.sleep(sleep)
        completableFuture.complete(accounts.toSet())
    }

    return completableFuture
}

private fun existsOrPut(transaction: Transaction): CompletableFuture<Boolean> {
    val completableFuture = CompletableFuture<Boolean>()

    DYNAMO_DB_THREAD_POOL.submit {
        val sleep = Random().asKotlinRandom().nextLong(DYNAMO_DB_MINIMUM_LATENCY, DYNAMO_DB_MAXIMUM_LATENCY)
        Thread.sleep(sleep)
        completableFuture.complete(false)
    }

    return completableFuture
}