package de.pavloff.spark4knime.transformations;

import org.knime.core.node.NodeView;

import de.pavloff.spark4knime.TableCellUtils.RddViewer;

/**
 * <code>NodeView</code> for the "Union" Node. Creates a new RDD that contains
 * the union of two RDD's
 * 
 * @author Oleg Pavlov, University of Heidelberg
 */
public class UnionNodeView extends NodeView<UnionNodeModel> {

	/**
	 * Creates a new view.
	 * 
	 * @param nodeModel
	 *            The model (class: {@link UnionNodeModel})
	 */
	protected UnionNodeView(final UnionNodeModel nodeModel) {
		super(nodeModel);
		// instantiate the components of the view here.
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void modelChanged() {
		// retrieve the new model from your nodemodel and update the view.
		UnionNodeModel nodeModel = (UnionNodeModel) getNodeModel();
		assert nodeModel != null;

		// be aware of a possibly not executed nodeModel! The data you retrieve
		// from your nodemodel could be null, emtpy, or invalid in any kind.
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void onClose() {
		// things to do when closing the view
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void onOpen() {
		// things to do when opening the view
		UnionNodeModel nodeModel = (UnionNodeModel) getNodeModel();
		assert nodeModel != null;
		RddViewer view = nodeModel.getRddViewer();
		assert (view != null);

		setComponent(view.getTableView());
	}

}
