import streamlit as st
import main
import asyncio
from database.connection import get_db_pool

# Define the pages
PAGES = {
    "Manage Containers": "manage_containers",
    "Start Operation": "start_operation",
    "Track Progress": "track_progress",
    "Analysis": "analysis"
}

# Sidebar for navigation
st.sidebar.title('Navigation')
selection = st.sidebar.radio("Go to", list(PAGES.keys()))


def manage_containers():
    st.title("Manage Keywords")
    
    # Add Container Section
    st.subheader('Add New Container')
    container_name = st.text_input('Container Name')
    container_description = st.text_area('Container Description')
    if st.button('Create Container'):
        main.create_container(container_name, container_description)
        st.success(f'Container {container_name} created successfully!')

    st.subheader('Add Keywords to Container')
    container_options = main.get_containers()
    selected_container = st.selectbox('Select a Container', container_options, format_func=lambda x: x[1])
    keywords_text_area = st.text_area('Enter keywords (each keyword on a new line)')
    if st.button('Add Keywords'):
        # Split the text area content into lines and filter out any empty lines
        keywords_list = keywords_text_area.split('\n')
        main.add_keywords_to_container(keywords_list, selected_container[0])
        st.success(f'Keywords added to {selected_container[1]}!')



def track_progress_page():
    st.title('Track Progress')
    
    # When the page loads or the container_id is entered
    message, progress = main.fetch_latest_progress()
    st.write(message)
    st.write(f"Progress: {progress} %")
    st.progress(progress / 100)

    # Optionally, add a button to refresh progress manually
    if st.button('Refresh Progress'):
        st.rerun()


def start_operation_page():
    st.title("Start Operation")
    # Change the input to accept numbers only
    container_id_input = st.text_input("Enter Container ID:", key="container_id")
    
    # Check if the input is not empty and is a digit
    if container_id_input and container_id_input.isdigit():
        # Convert container_id to an integer
        container_id = int(container_id_input)
        
        if st.button("Start Analysis"):
            if not main.is_operation_running():
                st.session_state.running = True
                # Assuming the analyze_data function is defined in a module named 'main'
                st.success(f"Analysis started for Container ID: {container_id}!")
                asyncio.run(main.analyze_data(container_id))
                
            else:
                st.warning("An analysis is already running for this Container ID.")
    elif container_id_input:
        # User has entered a non-numeric value
        st.error("Please enter a valid numeric Container ID.")

def analysis():
    st.title("Analysis")
    
    # Placeholder for analysis components
    st.write("Analysis tools and charts will be displayed here.")


# Page routing
if selection == "Manage Containers":
    manage_containers()
elif selection == "Start Operation":
    start_operation_page()
elif selection == "Track Progress":
    track_progress_page()
elif selection == "Analysis":
    analysis()


