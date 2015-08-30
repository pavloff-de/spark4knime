package de.pavloff.spark4knime.transformations;

import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeFactory;
import org.knime.core.node.NodeView;

/**
 * <code>NodeFactory</code> for the "SortByKey" Node.
 * returns a dataset of (K, V) pairs sorted by keys in ascending or descending order
 *
 * @author Oleg Pavlov, University of Heidelberg
 */
public class SortByKeyNodeFactory 
        extends NodeFactory<SortByKeyNodeModel> {

    /**
     * {@inheritDoc}
     */
    @Override
    public SortByKeyNodeModel createNodeModel() {
        return new SortByKeyNodeModel();
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
    public NodeView<SortByKeyNodeModel> createNodeView(final int viewIndex,
            final SortByKeyNodeModel nodeModel) {
        return new SortByKeyNodeView(nodeModel);
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
        return new SortByKeyNodeDialog();
    }

}

