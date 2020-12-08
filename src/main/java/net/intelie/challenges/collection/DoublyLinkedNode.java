package net.intelie.challenges.collection;

public class DoublyLinkedNode<T extends Comparable<T>>{
	private T element;
	private DoublyLinkedNode<T> nextNode;
	private DoublyLinkedNode<T> previousNode;
	
	public DoublyLinkedNode(T element, DoublyLinkedNode<T> previousNode, DoublyLinkedNode<T> nextNode) {
		this.element = element;
		this.previousNode = previousNode;
		this.nextNode = nextNode;
	}
	
	public T getElement() {
		return element;
	}
	public void setElement(T element) {
		this.element = element;
	}
	
	public DoublyLinkedNode<T> getNextNode() {
		return nextNode;
	}
	public void setNextNode(DoublyLinkedNode<T> nextNode) {
		this.nextNode = nextNode;
	}
	
	public DoublyLinkedNode<T> getPreviousNode() {
		return previousNode;
	}
	public void setPreviousNode(DoublyLinkedNode<T> previousNode) {
		this.previousNode = previousNode;
	}

}
