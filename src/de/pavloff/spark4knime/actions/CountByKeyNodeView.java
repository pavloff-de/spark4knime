package de.pavloff.spark4knime.actions;

import org.knime.core.node.NodeView;

import de.pavloff.spark4knime.TableCellUtils.RddViewer;

/**
 * <code>NodeView</code> for the "CountByKey" Node. Make a hashmap of (Key, Int)
 * pairs with the count of each key. Only for pairRDD available.
 * 
 * @author Oleg Pavlov, University of Heidelberg
 */
public class CountByKeyNodeView extends NodeView<CountByKeyNodeModel> {

	/**
	 * Creates a new view.
	 * 
	 * @param nodeModel
	 *            The model (class: {@link CountByKeyNodeModel})
	 */
	protected CountByKeyNodeView(final CountByKeyNodeModel nodeModel) {
		super(nodeModel);
		// instantiate the components of the view here.
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void modelChanged() {
		// retrieve the new model from your nodemodel and update the view.
		CountByKeyNodeModel nodeModel = (CountByKeyNodeModel) getNodeModel();
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
		CountByKeyNodeModel nodeModel = (CountByKeyNodeModel) getNodeModel();
		assert nodeModel != null;
		RddViewer view = nodeModel.getRddViewer();
		assert (view != null);

		setComponent(view.getTableView());
	}

}
