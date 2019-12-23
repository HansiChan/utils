from pywinauto import Application
import configparser


def VPNConnect():

    try:
        con_orig = Application().connect(title=u'VPN Connect - 119.147.211.38', timeout=1)
        con_orig.kill()
    except:
        pass

    conf = configparser.ConfigParser()
    conf.read("./conf.ini")

    App_dir = conf.get('config', 'dir')
    un = conf.get('config', 'username')
    pw = conf.get('config', 'password')

    app = Application().start(cmd_line=App_dir)
    shrewsoftacc = app.SHREWSOFT_ACC
    shrewsoftacc.wait('ready')
    syslistview = shrewsoftacc[u'ListView119.147.211.38']
    listview_item = syslistview.get_item(u'119.147.211.38')
    listview_item.click()

    toolbarwindow = shrewsoftacc[u'Toolbar']
    toolbar_button = toolbarwindow.button(u'Connect')
    toolbar_button.click()

    con = Application().connect(title=u'VPN Connect - 119.147.211.38', timeout=2)
    shrewsoftcon = con.SHREWSOFT_CON
    User = shrewsoftcon.Edit
    User.type_keys(un)
    Password = shrewsoftcon.Edit2
    Password.type_keys(pw)
    Connect = shrewsoftcon.Button
    Connect.click()

    app.kill()


if __name__ == "__main__":
    VPNConnect()
