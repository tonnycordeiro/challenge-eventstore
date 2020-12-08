package net.intelie.challenges;

import net.intelie.challenges.collection.DoublyLinkedNode;
import net.intelie.challenges.collection.SortedDoublyLinkedList;

/* It was chosen to iterate in a doubly linked list due to the following reasons:  
 * - It's a good way to remove, get and iterate when there is a pointer to some object.
 */

public class ConcreteEventIterator implements EventIterator{
	private DoublyLinkedNode<Event> currentNode;
	private SortedDoublyLinkedList<Event> sortedList;
	private long endTime;
	private boolean wasMovedNext;

	public ConcreteEventIterator() {
		this.currentNode = null;
		this.wasMovedNext = false;
	}
	
	/***
	 * 
	 * @param list: Doubly Linked List of events that will be sorted by insertion
	 * @param startTime: Start timestamp (inclusive)
	 * @param endTime: End timestamp (exclusive)
	 */
	public ConcreteEventIterator(SortedDoublyLinkedList<Event> list, long startTime, long endTime) {
		DoublyLinkedNode<Event> firstNode = list.getHeadNode();
		this.sortedList = list;
		this.endTime = endTime;
		this.wasMovedNext = false;
		
		while(((Event)firstNode.getElement()).timestamp() < startTime && moveNext());
		this.currentNode = new DoublyLinkedNode<Event>(null, null, firstNode);
	}
	
	@Override
	public void close() throws Exception {
		this.currentNode = null;
		this.sortedList = null;
	}
	
	@Override
	public boolean moveNext() {
		long currentTimeStamp = -1;
		if(this.sortedList == null)
			return false;
		this.currentNode = this.sortedList.moveNext(this.currentNode);
		if(currentNode != null)
			currentTimeStamp = ((Event)this.currentNode.getElement()).timestamp();
		this.wasMovedNext = (this.currentNode != null && currentTimeStamp < endTime); 
		return this.wasMovedNext;
	}

	@Override
	public Event current() {
		if(!this.wasMovedNext)
			throw new IllegalStateException();
		
		return (Event)this.currentNode.getElement();
	}

	@Override
	public void remove() {
		if(!this.wasMovedNext)
			throw new IllegalStateException();
		
		DoublyLinkedNode<Event> previousNode = this.currentNode.getPreviousNode();
		
		if(previousNode != null && previousNode.getElement() == null) {
			previousNode = null;
			this.currentNode.setPreviousNode(previousNode);
		}
		
		this.sortedList.remove(this.currentNode);
		
		this.currentNode = previousNode;
		if(this.currentNode == null && this.sortedList.getHeadNode() != null) //head // || this.currentNode.getElement() == null
			this.currentNode = new DoublyLinkedNode<Event>(null, null, this.sortedList.getHeadNode());
	}
}
