/*
 * Colton Morley
 * par_sumsq.c
 * Created from edited version of sumsq.c
 * CS 446.646 Project 5 (Pthreads)
 *
 * Compile with --std=c99
 */

#include <limits.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <pthread.h>

#include <time.h>

// aggregate variables
volatile long sum = 0;
volatile long odd = 0;
volatile long min = INT_MAX;
volatile long max = INT_MIN;
volatile bool done = false;

//Mutex for aggregate variable
pthread_mutex_t variableLock = PTHREAD_MUTEX_INITIALIZER;
//Mutex for queue
pthread_mutex_t queueLock = PTHREAD_MUTEX_INITIALIZER;

//Conditional variable for a task added to queue
pthread_cond_t addCond = PTHREAD_COND_INITIALIZER;

//Define nodes to be used in linked list
typedef struct Node
{
  long data;
  char action;
  struct Node *next;
} Node;

//Define singley-linked list itself
typedef struct MasterQueue
{
  Node *head;
} MasterQueue;

// function prototypes
void calculate_square(long number);

//Function to define attributes of worker threads
void *worker_attr(void *masterQ);

//Function to dynamically Allocate a new MasterQueue
MasterQueue *new_queue();

//Frees memory from allocated by queue
void destroyQueue(MasterQueue *q);

//Push task to queue
void push(MasterQueue *q, long data, char action);

//Pop task from queue, return 0 for empty queue, otherwise return popped data
long pop(MasterQueue *q);

int main(int argc, char *argv[])
{
  // check and parse command line options
  if (argc != 3)
  {
    printf("Usage: sumsq <infile> <Number of Worker Threads>\n");
    exit(EXIT_FAILURE);
  }

  //convert threads parameter from ascii to int
  int param = atoi(argv[2]);

  // check that parameter is positive
  if (param < 0)
  {
    printf("Number of worker threads must be positive\n");
    exit(EXIT_FAILURE);
  }

  char *fn = argv[1];

  // load numbers and add them to the queue
  FILE *fin = fopen(fn, "r");
  char action;
  long num;
  //Added test to make sure file open succesful
  if (!fin)
  {
    printf("File opening failed...Exiting\n");
    exit(EXIT_FAILURE);
  }

  //Create volatile MasterQueue
  volatile MasterQueue *queue = new_queue();

  //Create worker threads accodring to parameter
  pthread_t workers[param];

  //Read input from file
  while (fscanf(fin, "%c %ld\n", &action, &num) == 2)
  {
    push((MasterQueue *)queue, num, action);
  }

  fclose(fin);

  for (int i = 0; i < param; i++)
  {
    pthread_create(&workers[i], NULL, worker_attr, (void *)queue);
  }


  while (true)
  {
    pthread_mutex_lock(&queueLock);
    if (queue->head == NULL)
    {
      pthread_mutex_unlock(&queueLock);
      break;
    }
    if (queue->head->action == 'w')
    {

      pthread_mutex_unlock(&queueLock);
      int n = pop(queue);
      // wait, nothing new happening
      sleep(n);
    }
    else
    {
      pthread_mutex_unlock(&queueLock);
    }
  }


  //Signal that process is done
  done = true;

  //Clean up
  void *status;
  bool issue = false;
  for (int i = 0; i < param; i++)
  {
    pthread_join(workers[i], &status);
    issue = (status != EXIT_SUCCESS);
  }
  //destroyQueue((MasterQueue *)queue);
  if (issue)
  {
    printf("Error joining threads during cleanup");
  }

  // print results
  printf("%ld %ld %ld %ld\n", sum, odd, min, max);

  return (EXIT_SUCCESS);
}

//-----------------------
//Funtion Implementations
//-----------------------

//Update global variables
void calculate_square(long number)
{

  // calculate the square
  long the_square = number * number;

  // ok that was not so hard, but let's pretend it was
  // simulate how hard it is to square this number!
  sleep(number);

  //Add lock to aggreate variables
  pthread_mutex_lock(&variableLock);

  // let's add this to our (global) sum
  sum += the_square;

  // now we also tabulate some (meaningless) statistics
  if (number % 2 == 1)
  {
    // how many of our numbers were odd?
    odd++;
  }

  // what was the smallest one we had to deal with?
  if (number < min)
  {
    min = number;
  }

  // and what was the biggest one?
  if (number > max)
  {
    max = number;
  }

  //Unlock aggregate variables
  pthread_mutex_unlock(&variableLock);
}

//Function to define attributes of worker threads
void *worker_attr(void *masterQ)
{
  MasterQueue *q = (MasterQueue *)masterQ;
  //Loop through the process until Master says done
  while (true)
  {
    //Lock queue
    pthread_mutex_lock(&queueLock);
    if (q->head == NULL)
    {
      pthread_mutex_unlock(&queueLock);
      break;
    }
    if (q->head->action == 'p') // process, do some work
    {
      //Pop task to begin processing it
      long n = pop(q);
      //Release lock now that it is finished modifying queue
      pthread_mutex_unlock(&queueLock);
      //Calculate the square
      calculate_square(n);
    }
    else
    {
      pthread_mutex_unlock(&queueLock);
    }

    //Return success when loop is complete
  }
  return (EXIT_SUCCESS);
}

//Function to dynamically Allocate a new MasterQueue
MasterQueue *new_queue()
{
  //allocate memory for new queue
  MasterQueue *q = malloc(sizeof(MasterQueue));
  //Check for succesful allocation
  if (!q)
  {
    printf("Allocation Failure creating MasterQueue\n");
    return q;
  }
  //Start by default with head pointing to NULL
  q->head = NULL;
  return q;
}

//Frees memory from allocated by queue
void destroyQueue(MasterQueue *q)
{
  //Make sure queue is not already deallocated
  if (!q)
  {
    return;
  }
  //Delete nodes one by one
  while (q->head != NULL)
  {
    pop(q);
  }
  //Deallocate queue
  free(q);
}

//Push task to queue
void push(MasterQueue *q, long data, char action)
{
  //Allocate a new node
  Node *n = malloc(sizeof(Node));
  //Check for succesful allocation
  if (!n)
  {
    printf("Allocation Failure creating Queue Node\n");
    return;
  }
  //Put data into new node and set next pointer
  //Making pointer null because it will be last node in linked list
  n->data = data;
  n->action = action;
  n->next = NULL;
  //Check if queue is empty, if it is make it the head
  if (q->head == NULL)
  {
    q->head = n;
  }
  else
  {
    //If not, n needs to go to back of queue
    Node *temp = q->head;
    while (temp->next != NULL)
    {
      temp = temp->next;
    }
    //Add new node to the 'next' pointer of last node
    temp->next = n;
  }
}

//Pop task from queue, return 0 for empty queue, otherwise return popped data
long pop(MasterQueue *q)
{
  //Get adress of head to be popped
  Node *n = q->head;
  //Make sure not empty
  if (n != NULL)
  {
    q->head = n->next;
  }
  else
  {
    return NULL;
  }
  //Save data to return
  long x = n->data;
  //Free node and return data
  free(n);
  return x;
}