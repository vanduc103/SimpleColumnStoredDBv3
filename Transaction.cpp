/*
 * Transaction.cpp
 *
 *  Created on: Nov 11, 2016
 *      Author: duclv
 */

#include "Transaction.h"
#include <stdio.h>
#include <iostream>

namespace std {

Transaction::Transaction() {
	vecUndo = new vector<undoSpace>();
}

Transaction::~Transaction() {
	for (size_t i = 0; i < vecUndo->size(); i++) {
		delete vecUndo->at(i).delta;
		delete vecUndo->at(i).dataColumn;
		delete vecUndo->at(i).hashtable;
		delete vecUndo->at(i).versionColumn;
		delete vecUndo->at(i).versionVecValue;
	}
	delete vecUndo;
}

vector<Transaction::transaction*>* Transaction::vecTransaction = new vector<Transaction::transaction*>();
vector<size_t>* Transaction::vecActiveTransaction = new vector<size_t>();
vector<size_t>* Transaction::vecWaitingTransaction = new vector<size_t>();

size_t Transaction::createTx() {
	transaction* newTx = new transaction();
	// current time in miliseconds
	newTx->txnId = chrono::duration_cast < chrono::milliseconds
			> (chrono::steady_clock::now().time_since_epoch()).count();
	newTx->startTs = 0;
	newTx->csn = 0;
	newTx->status = TRANSACTION_STATUS::WAITING;
	vecTransaction->push_back(newTx);

	// return index to this new transaction
	return vecTransaction->size() - 1;
}

void Transaction::startTx(size_t txIdx) {
	transaction* tx = vecTransaction->at(txIdx);
	tx->status = TRANSACTION_STATUS::STARTED;
	tx->startTs = chrono::duration_cast < chrono::milliseconds
			> (chrono::steady_clock::now().time_since_epoch()).count();
	vecTransaction->at(txIdx) = tx;
	//copy to active transaction
	vecActiveTransaction->push_back(txIdx);
}

void Transaction::updateRid2Transaction(size_t txIdx, vector<size_t> vecRid) {
	transaction* tx = vecTransaction->at(txIdx);
	tx->vecRid = vecRid;
	vecTransaction->at(txIdx) = tx;
}

uint64_t Transaction::getStartTimestamp(size_t txIdx) {
	transaction* tx = vecTransaction->at(txIdx);
	return tx->startTs;
}

uint64_t Transaction::getTimestampAsCSN() {
	return chrono::duration_cast < chrono::milliseconds
			> (chrono::steady_clock::now().time_since_epoch()).count();
}

void Transaction::commitTx(size_t txIdx, uint64_t csn) {
	transaction* tx = vecTransaction->at(txIdx);
	tx->status = TRANSACTION_STATUS::COMMITED;
	tx->csn = csn;
	vecTransaction->at(txIdx) = tx;
	// remove in active transaction
	for (size_t i = 0; i < vecActiveTransaction->size(); i++) {
		if (vecActiveTransaction->at(i) == txIdx) {
			vecActiveTransaction->erase(vecActiveTransaction->begin() + i);
			break;
		}
	}
	// remove in waiting list
	for (size_t i = 0; i < vecWaitingTransaction->size(); i++) {
		if (vecWaitingTransaction->at(i) == txIdx) {
			vecWaitingTransaction->erase(vecWaitingTransaction->begin() + i);
			break;
		}
	}
}

void Transaction::abortTx(size_t txIdx) {
	transaction* tx = vecTransaction->at(txIdx);
	tx->status = TRANSACTION_STATUS::ABORTED;
	tx->csn = 0;
	vecTransaction->at(txIdx) = tx;
	// remove in active transaction
	for (size_t i = 0; i < vecActiveTransaction->size(); i++) {
		if (vecActiveTransaction->at(i) == txIdx) {
			vecActiveTransaction->erase(vecActiveTransaction->begin() + i);
			break;
		}
	}
}

vector<size_t> Transaction::listActiveTransaction() {
	return *vecActiveTransaction;
}

Transaction::transaction Transaction::getTransaction(size_t txIdx) {
	return *vecTransaction->at(txIdx);
}

void Transaction::addToWaitingList(size_t txIdx) {
	transaction* tx = vecTransaction->at(txIdx);
	tx->status = TRANSACTION_STATUS::WAITING;
	tx->startTs = 0;
	vecTransaction->at(txIdx) = tx;
	vecWaitingTransaction->push_back(txIdx);
}

vector<size_t> Transaction::getWaitingList() {
	return *vecWaitingTransaction;
}

void Transaction::setClient(size_t txIdx, ServerSocket* client) {
	transaction* tx = vecTransaction->at(txIdx);
	tx->client = client;
	vecTransaction->at(txIdx) = tx;
}

void Transaction::setCommand(size_t txIdx, vector<string> command) {
	transaction* tx = vecTransaction->at(txIdx);
	tx->command = command;
	vecTransaction->at(txIdx) = tx;
}

ServerSocket* Transaction::getClient(size_t txIdx) {
	return vecTransaction->at(txIdx)->client;
}

vector<string> Transaction::getCommand(size_t txIdx) {
	return vecTransaction->at(txIdx)->command;
}

Transaction::TRANSACTION_STATUS Transaction::getStatus(size_t txIdx) {
	return vecTransaction->at(txIdx)->status;
}

vector<size_t> Transaction::getVecRid(size_t txIdx) {
	return vecTransaction->at(txIdx)->vecRid;
}

void Transaction::saveUndoSpace(size_t txIdx, undoSpace undoValue) {
	vecUndo->push_back(undoValue);
}

vector<Transaction::undoSpace> Transaction::getUndoSpace(size_t txIdx) {
	vector<undoSpace> returnVecUndo;
	for (size_t i = 0; i < vecUndo->size(); i++) {
		if (vecUndo->at(i).txIdx == txIdx)
			returnVecUndo.push_back(vecUndo->at(i));
	}
	return returnVecUndo;
}

void Transaction::removeUndoSpace(size_t txIdx) {
	for (size_t i = 0; i < vecUndo->size(); i++) {
		if (vecUndo->at(i).txIdx == txIdx)
			vecUndo->erase(vecUndo->begin() + i);
	}
}

bool Transaction::checkExistsUndo(size_t txIdx) {
	for (size_t i = 0; i < vecUndo->size(); i++) {
		if (vecUndo->at(i).txIdx == txIdx)
			return true;
	}
	return false;
}

} /* namespace std */
