package de.pavloff.spark4knime.output;

import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeFactory;
import org.knime.core.node.NodeView;

/**
 * <code>NodeFactory</code> for the "RDDToSequenceFile" Node.
 * Save RDD as Sequence File
 *
 * @author Oleg Pavlov
 */
public class RDDToSequenceFileNodeFactory 
        extends NodeFactory<RDDToSequenceFileNodeModel> {

    /**
     * {@inheritDoc}
     */
    @Override
    public RDDToSequenceFileNodeModel createNodeModel() {
        return new RDDToSequenceFileNodeModel();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getNrNodeViews() {
        return 1;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NodeView<RDDToSequenceFileNodeModel> createNodeView(final int viewIndex,
            final RDDToSequenceFileNodeModel nodeModel) {
        return new RDDToSequenceFileNodeView(nodeModel);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean hasDialog() {
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NodeDialogPane createNodeDialogPane() {
        return new RDDToSequenceFileNodeDialog();
    }

}

