package de.pavloff.spark4knime.actions;

import org.knime.core.node.NodeView;

import de.pavloff.spark4knime.TableCellUtils.RddViewer;

/**
 * <code>NodeView</code> for the "Collect" Node. Collect all the elements of the
 * RDD as a table
 * 
 * @author Oleg Pavlov
 */
public class CollectNodeView extends NodeView<CollectNodeModel> {
	
	/**
	 * Creates a new view.
	 * 
	 * @param nodeModel
	 *            The model (class: {@link CollectNodeModel})
	 */
	protected CollectNodeView(final CollectNodeModel nodeModel) {
		super(nodeModel);
		
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void modelChanged() {

		// retrieve the new model from your nodemodel and
		// update the view.
		CollectNodeModel nodeModel = (CollectNodeModel) getNodeModel();
		assert nodeModel != null;
		
		// be aware of a possibly not executed nodeModel! The data you retrieve
		// from your nodemodel could be null, emtpy, or invalid in any kind.

	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void onClose() {
		
		// TODO things to do when closing the view
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void onOpen() {
		// things to do when opening the view
		CollectNodeModel nodeModel = (CollectNodeModel) getNodeModel();
		assert nodeModel != null;
		RddViewer view = nodeModel.getRddViewer();
		assert (view != null);
		setComponent(view.getTableView());
	}

}
