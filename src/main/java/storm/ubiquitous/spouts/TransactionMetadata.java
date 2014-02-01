/**
 * This metadata is the used for replaying each batch 
 * belonging to a partition. It is stored in the zookeeper 
 * by storm to manage replaying of messages.
 * It saves the partition no., starting offset for
 * the batch and the end offset for the batch.
 * The start and end offset depends on the size of 
 * the batch.
 */

package storm.ubiquitous.spouts;

import java.io.Serializable;

public class TransactionMetadata implements Serializable{
    private static final long serialVersionUID = 1L;
    public int partition;
    public long startoffset;
    public long endoffset;

    public TransactionMetadata(){
    }

    public TransactionMetadata(int partition,long startoffset,long endoffset){
	this.partition = partition;
	this.startoffset = startoffset;
	this.endoffset = endoffset;
    }
}
