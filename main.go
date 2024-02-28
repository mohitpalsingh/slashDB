package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

/*GlobalStore holds the (global) variables*/
var GlobalStore = make(map[string]string)

/*Transaction points to a key:value store*/
type Transaction struct {
	store map[string]string
	next  *Transaction
}

/*TransactionStack maintains a list of active/suspended transactions*/
type TransactionStack struct {
	top  *Transaction
	size int
}

/*PushTransaction creates a new active transaction*/
func (ts *TransactionStack) PushTransaction() {
	temp := Transaction{store: make(map[string]string), next: ts.top}
	ts.top = &temp
	ts.size++
}

/*PopTransaction removes a transaction from the stack*/
func (ts *TransactionStack) PopTransaction() {
	if ts.top == nil {
		fmt.Printf("ERROR: No Active Transactions\n")
	} else {
		node := &Transaction{}
		ts.top = ts.top.next
		node.next = nil
		ts.size--
	}
}

/*Peek returns the active transaction*/
func (ts *TransactionStack) Peek() *Transaction {
	return ts.top
}

/*Commit write(SET) changes to the store with TransactionStack scope */
func (ts *TransactionStack) Commit() {
	ActiveTransaction := ts.Peek()
	if ActiveTransaction != nil {
		for key, value := range ActiveTransaction.store {
			GlobalStore[key] = value
			if ActiveTransaction.next != nil {
				ActiveTransaction.next.store[key] = key
			}
		}
	} else {
		fmt.Print("INFO: Nothing to commit\n")
	}
}

/*RollbackTransaction clears keys SET within a transaction*/
func (ts *TransactionStack) RollbackTransaction() {
	if ts.top == nil {
		fmt.Printf("ERROR: No Active Transaction\n")
	} else {
		for key := range ts.top.store {
			delete(ts.top.store, key)
		}
	}
}

/*Get value of key from Store*/
func Get(key string, T *TransactionStack) {
	ActiveTransaction := T.Peek()
	if ActiveTransaction == nil {
		if val, ok := GlobalStore[key]; ok {
			fmt.Printf("%s\n", val)
		} else {
			fmt.Printf("%s is not set\n", key)
		}
	} else {
		if val, ok := ActiveTransaction.store[key]; ok {
			fmt.Printf("%s\n", val)
		} else {
			fmt.Printf("%s is not set\n", key)
		}
	}
}

/*Set value of key*/
func Set(key string, value string, T *TransactionStack) {
	ActiveTransaction := T.Peek()
	if ActiveTransaction == nil {
		GlobalStore[key] = value
	} else {
		ActiveTransaction.store[key] = value
	}
}

/* Delete key and it's value*/
func Delete(key string, T *TransactionStack) {
	ActiveTransaction := T.Peek()
	if ActiveTransaction == nil {
		delete(GlobalStore, key)
	} else {
		delete(ActiveTransaction.store, key)
	}
}

func main() {
	fmt.Printf("Welcome to SlashDB by Mohit Pal Singh!\n")
	reader := bufio.NewReader(os.Stdin)
	items := &TransactionStack{}
	for {
		fmt.Printf("> ")
		text, _ := reader.ReadString('\n')
		// splitting the operation strings
		operation := strings.Fields(text)
		switch operation[0] {
		case "BEGIN":
			items.PushTransaction()
		case "ROLLBACK":
			items.RollbackTransaction()
		case "COMMIT":
			items.Commit()
			items.PopTransaction()
		case "END":
			items.PopTransaction()
		case "SET":
			Set(operation[1], operation[2], items)
		case "GET":
			Get(operation[1], items)
		case "DELETE":
			Delete(operation[1], items)
		case "STOP":
			os.Exit(0)
		default:
			fmt.Printf("ERROR: Unrecognised operation %s\n", operation[0])
		}
	}
}
