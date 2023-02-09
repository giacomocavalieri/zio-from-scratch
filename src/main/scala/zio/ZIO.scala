package zio

import scala.annotation.tailrec

enum ZIO[+E, +A]:
  private[zio] case FlatMap[E, A, B](zio: ZIO[E, A], f: A => ZIO[E, B]) extends ZIO[E, B]
  private[zio] case Succeed[A](thunk: () => A)                          extends ZIO[Nothing, A]
  private[zio] case Async[E, A](register: (ZIO[E, A] => Unit) => Unit)  extends ZIO[E, A]
  private[zio] case Fail[E](cause: () => Cause[E])                      extends ZIO[E, Nothing]
  private[zio] case Fold[E, E1, A, B](
      zio: ZIO[E, A],
      onFailure: Cause[E] => ZIO[E1, B],
      onSuccess: A => ZIO[E1, B]
  )                                                                     extends ZIO[E1, B]

object ZIO:
  def succeed[A](a: => A): ZIO[Nothing, A]             = ZIO.Succeed(() => a)
  def failCause[E](cause: Cause[E]): ZIO[E, Nothing]   = ZIO.Fail(() => cause)
  def fail[E](error: => E): ZIO[E, Nothing]            = failCause(Cause.fail(error))
  def die(throwable: Throwable): ZIO[Nothing, Nothing] = failCause(Cause.die(throwable))
  def interrupt: ZIO[Nothing, Nothing]                 = failCause(Cause.interrupt)
  def done[E, A](exit: Exit[E, A]): ZIO[E, A]          = exit match
    case Exit.Success(a) => succeed(a)
    case Exit.Failure(e) => failCause(e)

  def async[E, A](register: (ZIO[E, A] => Unit) => Unit): ZIO[E, A] = ZIO.Async(register)

  def println(string: String): ZIO[Nothing, Unit] = ZIO.succeed(Predef.println(string))
  def readln: ZIO[Nothing, String]                = ZIO.succeed(scala.io.StdIn.readLine())

  def never: ZIO[Nothing, Nothing] = ZIO.async { continue => () }

  extension [E, A](zio: ZIO[E, A])
    def flatMap[E1 >: E, B](f: A => ZIO[E1, B]): ZIO[E1, B] = ZIO.FlatMap(zio, f)
    def map[B](f: A => B): ZIO[E, B]                        = flatMap(a => ZIO.succeed(f(a)))
    def fork: ZIO[Nothing, Fiber[E, A]]                     = ZIO.succeed {
      val fiber = FiberRuntime(zio)
      fiber.unsafeStart()
      fiber
    }

    def catchAll[E2, A1 >: A](handler: E => ZIO[E2, A1]): ZIO[E2, A1]             = fold(handler, ZIO.succeed)
    def catchAllCause[E2, A1 >: A](handler: Cause[E] => ZIO[E2, A1]): ZIO[E2, A1] = foldCause(handler, ZIO.succeed)
    def foldCause[E2, B](
        onFailure: Cause[E] => ZIO[E2, B],
        onSuccess: A => ZIO[E2, B]
    ): ZIO[E2, B] = ZIO.Fold(zio, onFailure, onSuccess)
    def fold[E2, B](
        onFailure: E => ZIO[E2, B],
        onSuccess: A => ZIO[E2, B]
    ): ZIO[E2, B] = foldCause(
      cause =>
        cause match
          case Cause.Fail(e)   => onFailure(e)
          case Cause.Die(t)    => ZIO.die(t)
          case Cause.Interrupt => ZIO.failCause(Cause.interrupt)
      ,
      onSuccess
    )

    def zipWith[E1 >: E, B, C](that: => ZIO[E1, B])(f: (A, B) => C): ZIO[E1, C] = flatMap(a => that.map(b => f(a, b)))
    def zip[E1 >: E, B](that: => ZIO[E1, B]): ZIO[E1, (A, B)]                   = zipWith(that)((_, _))
    def zipRight[E1 >: E, B](that: => ZIO[E1, B]): ZIO[E1, B]                   = zipWith(that)((_, b) => b)
    def *>[E1 >: E, B](that: => ZIO[E1, B]): ZIO[E1, B]                         = zipRight(that)
    def zipPar[E1 >: E, B](that: ZIO[E1, B]): ZIO[E1, (A, B)]                   = zipWithPar(that)((_, _))
    def zipWithPar[E1 >: E, B, C](that: ZIO[E1, B])(f: (A, B) => C): ZIO[E1, C] = for
      fiberA <- zio.fork
      fiberB <- that.fork
      a      <- fiberA.join
      b      <- fiberB.join
    yield f(a, b)

    def forever[B]: ZIO[E, B]            = zio *> forever
    def repeat(times: Int): ZIO[E, Unit] = if times <= 0 then ZIO.succeed(()) else zio *> repeat(times - 1)

    def unsafeRunSync(): Exit[E, A] =
      val latch   = java.util.concurrent.CountDownLatch(1)
      var result  = null.asInstanceOf[Exit[E, A]]
      val program = zio.foldCause(
        cause =>
          ZIO.fail {
            result = Exit.Failure(cause)
            latch.countDown()
          },
        a =>
          ZIO.succeed {
            result = Exit.Success(a)
            latch.countDown()
          }
      )
      program.unsafeRunAsync()
      latch.await()
      result

    def unsafeRunAsync(): Unit = // Fire and forget
      val fiber = FiberRuntime(zio)
      fiber.unsafeStart()

// The in flight computation
trait Fiber[+E, +A]:
  def await: ZIO[Nothing, Exit[E, A]]
  def interruptFork: ZIO[Nothing, Unit] // send interrupt signal and return immediately
  final def join: ZIO[E, A]                     = await.flatMap(ZIO.done)
  final def interrupt: ZIO[Nothing, Exit[E, A]] = interruptFork *> await // wait for interruption to be processed

enum Cause[+E]:
  case Fail[E](e: E)             extends Cause[E]
  case Die(throwable: Throwable) extends Cause[Nothing]
  case Interrupt                 extends Cause[Nothing]

object Cause:
  def fail[E](e: => E): Cause[E]                = Cause.Fail(e)
  def die(throwable: Throwable): Cause[Nothing] = Cause.Die(throwable)
  def interrupt: Cause[Nothing]                 = Cause.Interrupt

enum Exit[+E, +A]:
  case Success[A](a: A)        extends Exit[Nothing, A]
  case Failure[E](e: Cause[E]) extends Exit[E, Nothing]

private[zio] final case class FiberRuntime[E, A](zio: ZIO[E, A]) extends Fiber[E, A]:
  private enum FiberMessage:
    case Start
    case AddObserver(callback: Exit[Any, Any] => Unit)
    case Resume(zio: ZIO[Any, Any])
    case Interrupt

  private enum Continuation:
    case OnSuccess(onSuccess: Any => ZIO[Any, Any])
    case OnSuccessAndFailure(onSuccess: Any => ZIO[Any, Any], onFailure: Any => ZIO[Any, Any])

  extension (continuation: Continuation)
    private def onSuccess: Any => ZIO[Any, Any] = continuation match
      case Continuation.OnSuccess(onSuccess)              => onSuccess
      case Continuation.OnSuccessAndFailure(onSuccess, _) => onSuccess

  private val stack      = scala.collection.mutable.Stack.empty[Continuation]
  private var currentZIO = zio.asInstanceOf[ZIO[Any, Any]]
  private var result     = null.asInstanceOf[Exit[Any, Any]]
  private var observers  = Set.empty[Exit[Any, Any] => Unit]
  private val executor   = scala.concurrent.ExecutionContext.global
  private val inbox      = java.util.concurrent.ConcurrentLinkedQueue[FiberMessage]
  private val running    = java.util.concurrent.atomic.AtomicBoolean(false)

  private def erase[E, A, B](continuation: A => ZIO[E, B]): Any => ZIO[Any, Any] =
    continuation.asInstanceOf[Any => ZIO[Any, Any]]

  private def offerToInbox(message: FiberMessage): Unit =
    inbox.add(message)
    if running.compareAndSet(false, true)
    then drainQueueOnNewExecutor()

  private def drainQueueOnNewExecutor(): Unit =
    executor.execute(() => drainQueueOnCurrentExecutor())

  @tailrec private def drainQueueOnCurrentExecutor(): Unit =
    val message = inbox.poll
    if message ne null then
      processFiberMessage(message)
      drainQueueOnCurrentExecutor()
    else
      running.set(false)
      if !inbox.isEmpty
      then
        if running.compareAndSet(false, true)
        then drainQueueOnCurrentExecutor()

  private def processFiberMessage(message: FiberMessage): Unit = message match
    case FiberMessage.AddObserver(callback) => if result != null then callback(result) else observers += callback
    case FiberMessage.Resume(zio)           => currentZIO = zio; runLoop
    case FiberMessage.Start                 => runLoop
    case FiberMessage.Interrupt             =>
      if result eq null then
        currentZIO = ZIO.interrupt
        runLoop

  private def maxOps        = 300
  private def runLoop: Unit =
    var loop    = true
    var opCount = 0
    while loop do
      opCount += 1
      if opCount >= maxOps then
        loop = false
        offerToInbox(FiberMessage.Resume(currentZIO))
        currentZIO = null
        return

      try
        currentZIO match
          case ZIO.Succeed(thunk) =>
            val value = thunk()
            if stack.isEmpty then
              result = Exit.Success(value)
              loop = false
              observers.foreach(callback => callback(result))
              observers = Set.empty
            else
              val continuation = stack.pop
              currentZIO = continuation.onSuccess(value)

          case ZIO.Fail(error) =>
            val continuation = findNextErrorHandler()
            if continuation == null then
              result = Exit.Failure(error())
              loop = false
              observers.foreach(callback => callback(result))
              observers = Set.empty
            else currentZIO = continuation.onFailure(error())

          case ZIO.FlatMap(zio, f) =>
            currentZIO = zio
            stack.push(Continuation.OnSuccess(erase(f)))

          case ZIO.Fold(zio, onFailure, onSuccess) =>
            currentZIO = zio
            stack.push(Continuation.OnSuccessAndFailure(erase(onSuccess), erase(onFailure)))

          case ZIO.Async(register) =>
            currentZIO = null
            loop = false
            register(zio => offerToInbox(FiberMessage.Resume(zio)))
      catch case t: Throwable => currentZIO = ZIO.die(t)

  private def findNextErrorHandler(): Continuation.OnSuccessAndFailure =
    var loop         = true
    var continuation = null.asInstanceOf[Continuation.OnSuccessAndFailure]
    while loop && !stack.isEmpty do
      stack.pop match
        case Continuation.OnSuccess(_)                  =>
        case c @ Continuation.OnSuccessAndFailure(_, _) =>
          continuation = c
          loop = false
    continuation

  def unsafeStart(): Unit                             = offerToInbox(FiberMessage.Start)
  def unsafeAddObserver(callback: Exit[E, A] => Unit) =
    offerToInbox(FiberMessage.AddObserver(callback.asInstanceOf[Exit[Any, Any] => Unit]))

  override def interruptFork: ZIO[Nothing, Unit] = ZIO.succeed(offerToInbox(FiberMessage.Interrupt))
  override def await: ZIO[Nothing, Exit[E, A]]   =
    ZIO.async { complete =>
      unsafeAddObserver { exit =>
        complete(ZIO.succeed(exit))
      }
    }

/* Some examples!














 */
object Example:

  def getIntAsync(callback: Int => Unit): Unit =
    Thread.sleep(3000)
    callback(10)

  @main def succeed =
    val program = for
      res <- ZIO.succeed(2)
      _   <- ZIO.println("FINE")
    yield res

    println(program.repeat(10000).unsafeRunSync())

  @main def async =
    val program = for
      _   <- ZIO.println("Calling legacy callback code")
      int <- ZIO.async[Nothing, Int] { complete =>
               getIntAsync { i =>
                 complete(ZIO.succeed(i))
               }
             }
      _   <- ZIO.println("Done")
    yield ()
    println(program.unsafeRunSync())

@main def fork =
  def program(string: String)(seconds: Int) = for
    _ <- ZIO.succeed(Thread.sleep(1000 * seconds))
    _ <- ZIO.println(string)
  yield seconds

  val program1 = program("Hello")(10)
  val program2 = program("World")(5)
  println(program1.zipPar(program2).unsafeRunSync())

@main def fail =
  val program = for
    _ <- ZIO.println("Hello")
    _ <- ZIO.fail(1)
    _ <- ZIO.println("World")
  yield 10
  println(program.unsafeRunSync())

@main def die =
  val program = for
    _ <- ZIO.println("Die")
    _ <- ZIO.die(IllegalStateException())
    _ <- ZIO.println("unreachable!")
  yield 10
  println(program.unsafeRunSync())

@main def interruption =
  val program = for
    fiber <- ZIO.never.fork
    _     <- ZIO.readln
    _     <- fiber.interrupt
  yield ()
  println(program.unsafeRunSync())

@main def prova =
  val dots    = ZIO.succeed(".").forever
  val program = for
    _     <- ZIO.println("Starting")
    fiber <- dots.fork
    _     <- ZIO.println("Forked")
    _     <- ZIO.succeed(Thread.sleep(1000))
    _     <- fiber.interrupt
  yield ()
  println(program.unsafeRunSync())
