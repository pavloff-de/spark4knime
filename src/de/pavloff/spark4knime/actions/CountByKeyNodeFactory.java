package de.pavloff.spark4knime.actions;

import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeFactory;
import org.knime.core.node.NodeView;

/**
 * <code>NodeFactory</code> for the "CountByKey" Node.
 * Make a hash map of count for each key. Only for pairRDD available
 *
 * @author Oleg Pavlov
 */
public class CountByKeyNodeFactory 
        extends NodeFactory<CountByKeyNodeModel> {

    /**
     * {@inheritDoc}
     */
    @Override
    public CountByKeyNodeModel createNodeModel() {
        return new CountByKeyNodeModel();
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
    public NodeView<CountByKeyNodeModel> createNodeView(final int viewIndex,
            final CountByKeyNodeModel nodeModel) {
        return new CountByKeyNodeView(nodeModel);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean hasDialog() {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NodeDialogPane createNodeDialogPane() {
        return new CountByKeyNodeDialog();
    }

}

