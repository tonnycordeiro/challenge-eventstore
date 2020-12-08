package net.intelie.challenges.collection;
import java.util.Collection;

/***
 * It was implemented a doubly linked list based on a generic type T: 
 * - there are two synchronized blocks to ensure thread-safe in insertion and deletion actions. 
 */
public class SortedDoublyLinkedList<T extends Comparable<T>> {
	private DoublyLinkedNode<T> headNode;
	
	public DoublyLinkedNode<T> getHeadNode(){
		return headNode;
	}

	public SortedDoublyLinkedList() {
		this.headNode = null;
	}
	
	public SortedDoublyLinkedList(Collection<T> objectCollection) {
		DoublyLinkedNode<T> previousNode = null;
		
		for (T element : objectCollection) {
			DoublyLinkedNode<T> currentNode = new DoublyLinkedNode<T>(element, previousNode, null);
			if(this.headNode == null)
				this.headNode = currentNode;
			if(previousNode != null)
				previousNode.setNextNode(currentNode);
			previousNode = currentNode;
		}
	}
	
	public void insert(T element) {
		DoublyLinkedNode<T> node = this.headNode;
		DoublyLinkedNode<T> previousNode = null;
		
		if(this.headNode == null) {
			this.headNode = new DoublyLinkedNode<T>(element, null, null);
			return;
		}
		
		//move nodes to inserting position
		while(node != null) {
			if(node.getElement().compareTo(element) < 0) {
				previousNode = node; 
				node = node.getNextNode();
			}else {
				break;
			}
		}
		
		synchronized (this) {
			// it is verified again if the node is being inserted in a correct position (a double-checked locking)
			while(node != null) {
				if(node.getElement().compareTo(element) < 0) {
					previousNode = node; 
					node = node.getNextNode();
				}else {
					break;
				}
			}
			if(previousNode != null) {
				previousNode.setNextNode(new DoublyLinkedNode<T>(element, previousNode, previousNode.getNextNode()));
			}else {
				this.headNode =  new DoublyLinkedNode<T>(element, null, headNode);
			}
		}
	}
	
	public DoublyLinkedNode<T> moveNext(DoublyLinkedNode<T> currentNode){
		if(currentNode != null)
			return currentNode.getNextNode();
		return null;
	}
	
	public void removeAll() {
		DoublyLinkedNode<T> node = this.headNode;
		DoublyLinkedNode<T> previousNode;
		while(node != null) {
			previousNode = node;
			node = node.getNextNode();
			remove(previousNode);
		}
	}
	
	public void remove(DoublyLinkedNode<T> node) {
		DoublyLinkedNode<T> nextNode;
		if(node == null)
			return; //exception
		
		synchronized (this) {
			DoublyLinkedNode<T> previousNode = node.getPreviousNode();
			
			if(previousNode != null) {
				previousNode.setNextNode(node.getNextNode());
				deleteNode(node);
			}else {
				if(node.equals(headNode)) {
					nextNode = this.headNode;
					this.headNode = this.headNode.getNextNode();
					deleteNode(nextNode);
				}
			}
		}
	}

	private void deleteNode(DoublyLinkedNode<T> node) {
		node.setElement(null);
		node.setNextNode(null);
		node.setPreviousNode(null);
		node = null;
	}
	
}
